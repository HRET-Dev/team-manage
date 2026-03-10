"""
兑换流程服务 (Redeem Flow Service)
协调兑换码验证, Team 选择和加入 Team 的完整流程
"""
import logging
import asyncio
import traceback
from collections import defaultdict
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from sqlalchemy import select, update, delete, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import RedemptionCode, RedemptionRecord, Team
from app.services.redemption import RedemptionService
from app.services.team import TeamService
from app.services.warranty import warranty_service
from app.services.notification import notification_service
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)

# 全局兑换锁: 针对 code 进行加锁，防止同一个码并发请求
_code_locks = defaultdict(asyncio.Lock)
# 全局 Team 锁: 针对 Team 进行加锁，防止并发拉人导致的人数状态不同步
_team_locks = defaultdict(asyncio.Lock)


class RedeemFlowService:
    """兑换流程场景服务类"""

    def __init__(self):
        """初始化兑换流程服务"""
        from app.services.chatgpt import chatgpt_service
        self.redemption_service = RedemptionService()
        self.warranty_service = warranty_service
        self.team_service = TeamService()
        self.chatgpt_service = chatgpt_service

    async def verify_code_and_get_teams(
        self,
        code: str,
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        验证兑换码并返回可用 Team 列表
        针对 aiosqlite 进行优化，避免 greenlet_spawn 报错
        """
        try:
            # 1. 验证兑换码
            validate_result = await self.redemption_service.validate_code(code, db_session)

            if not validate_result["success"]:
                return {
                    "success": False,
                    "valid": False,
                    "reason": None,
                    "teams": [],
                    "error": validate_result["error"]
                }
            
            # 如果是已经标记为过期了
            if not validate_result["valid"] and validate_result.get("reason") == "兑换码已过期 (超过首次兑换截止时间)":
                try:
                    await db_session.commit()
                except:
                    pass

            if not validate_result["valid"]:
                return {
                    "success": True,
                    "valid": False,
                    "reason": validate_result["reason"],
                    "teams": [],
                    "error": None
                }

            # 2. 获取可用 Team 列表
            teams_result = await self.team_service.get_available_teams(db_session)

            if not teams_result["success"]:
                return {
                    "success": False,
                    "valid": True,
                    "reason": "兑换码有效",
                    "teams": [],
                    "error": teams_result["error"]
                }

            logger.info(f"验证兑换码成功: {code}, 可用 Team 数量: {len(teams_result['teams'])}")

            return {
                "success": True,
                "valid": True,
                "reason": "兑换码有效",
                "teams": teams_result["teams"],
                "error": None
            }

        except Exception as e:
            logger.error(f"验证兑换码并获取 Team 列表失败: {e}")
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "valid": False,
                "reason": None,
                "teams": [],
                "error": f"验证失败: {str(e)}"
            }

    async def select_team_auto(
        self,
        db_session: AsyncSession,
        exclude_team_ids: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """
        自动选择一个可用的 Team
        """
        try:
            # 查找所有 active 且未满的 Team
            stmt = select(Team).where(
                Team.status == "active",
                Team.current_members < Team.max_members
            )
            
            if exclude_team_ids:
                stmt = stmt.where(Team.id.not_in(exclude_team_ids))
            
            # 优先选择人数最少的 Team (负载均衡)
            stmt = stmt.order_by(Team.current_members.asc(), Team.created_at.desc())
            
            result = await db_session.execute(stmt)
            team = result.scalars().first()

            if not team:
                reason = "没有可用的 Team"
                if exclude_team_ids:
                    reason = "您已加入所有可用 Team"
                return {
                    "success": False,
                    "team_id": None,
                    "error": reason
                }

            logger.info(f"自动选择 Team: {team.id}")

            return {
                "success": True,
                "team_id": team.id,
                "error": None
            }

        except Exception as e:
            logger.error(f"自动选择 Team 失败: {e}")
            return {
                "success": False,
                "team_id": None,
                "error": f"自动选择 Team 失败: {str(e)}"
            }

    async def redeem_and_join_team(
        self,
        email: str,
        code: str,
        team_id: Optional[int],
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        完整的兑换流程 (带事务和并发控制)
        """
        last_error = "未知错误"
        max_retries = 3
        current_target_team_id = team_id

        # 针对 code 加锁，防止同一个码并发进入兑换
        async with _code_locks[code]:
            for attempt in range(max_retries):
                logger.info(f"兑换尝试 {attempt + 1}/{max_retries} (Code: {code}, Email: {email})")
                
                try:
                    # 确定目标 Team (初选，具体锁定在 team_locks 中进行)
                    team_id_final = current_target_team_id
                    if not team_id_final:
                        select_res = await self.select_team_auto(db_session)
                        if not select_res["success"]:
                            return {"success": False, "error": select_res["error"]}
                        team_id_final = select_res["team_id"]
                        current_target_team_id = team_id_final
                    
                    # 使用 Team 锁序列化对该账户的操作，防止并发冲突 (ChatGPT 账号通常只允许单进程邀请)
                    async with _team_locks[team_id_final]:
                        logger.info(f"锁定 Team {team_id_final} 执行兑换流程")
                        
                        # 0. 确保 session 状态干净
                        if db_session.in_transaction():
                            await db_session.rollback()

                        # 1. 前置同步：拉人前确保人数状态绝对实时 (耗时操作，不开启事务)
                        sync_res = await self.team_service.sync_team_info(team_id_final, db_session)
                        
                        # 2. 核心校验 (开启短事务)
                        async with db_session.begin():
                            # 1. 验证和锁定码
                            stmt = select(RedemptionCode).where(RedemptionCode.code == code).with_for_update()
                            res = await db_session.execute(stmt)
                            rc = res.scalar_one_or_none()

                            if not rc:
                                return {"success": False, "error": "兑换码不存在"}
                            
                            if rc.status not in ["unused", "warranty_active"]:
                                if rc.status == "used":
                                    warranty_check = await self.warranty_service.validate_warranty_reuse(
                                        db_session, code, email
                                    )
                                    if not warranty_check.get("can_reuse"):
                                        return {"success": False, "error": warranty_check.get("reason") or "兑换码已使用"}
                                else:
                                    return {"success": False, "error": f"兑换码状态无效: {rc.status}"}

                            # 2. 锁定并校验 Team
                            stmt = select(Team).where(Team.id == team_id_final).with_for_update()
                            res = await db_session.execute(stmt)
                            target_team = res.scalar_one_or_none()
                            
                            if not target_team or target_team.status != "active":
                                raise Exception(f"目标 Team {team_id_final} 不可用 ({target_team.status if target_team else 'None'})")
                            
                            if target_team.current_members >= target_team.max_members:
                                target_team.status = "full"
                                raise Exception("该 Team 已满, 请选择其他 Team 尝试")

                            # 提交以此锁定状态（虽然接下来会执行 API，但由于有 _team_locks 保护，其他进程进不来）
                        
                        # 3. 执行 API 邀请 (耗时操作，放事务外，通过 _team_locks 保持原子性)
                        # 再次获取 token
                        access_token = await self.team_service.ensure_access_token(target_team, db_session)
                        if not access_token:
                            raise Exception("获取 Team 访问权限失败，账户状态异常")

                        invite_res = await self.chatgpt_service.send_invite(
                            access_token, target_team.account_id, email, db_session,
                            identifier=target_team.email
                        )
                        
                        # 4. 后置处理与状态持久化 (第二次短事务)
                        async with db_session.begin():
                            # 重新载入以防止 session 过期
                            res = await db_session.execute(select(RedemptionCode).where(RedemptionCode.code == code).with_for_update())
                            rc = res.scalar_one_or_none()
                            res = await db_session.execute(select(Team).where(Team.id == team_id_final).with_for_update())
                            target_team = res.scalar_one_or_none()

                            if not invite_res["success"]:
                                err = invite_res.get("error", "邀请失败")
                                err_str = str(err).lower()
                                if any(kw in err_str for kw in ["already in workspace", "already in team", "already a member"]):
                                    logger.info(f"用户 {email} 已经在 Team {team_id_final} 中，视为兑换成功")
                                else:
                                    if any(kw in err_str for kw in ["maximum number of seats", "full", "no seats"]):
                                        target_team.status = "full"
                                        raise Exception(f"该 Team 席位已满 (API Error: {err})")
                                    raise Exception(err)

                            # 成功逻辑
                            rc.status = "used"
                            rc.used_by_email = email
                            rc.used_team_id = team_id_final
                            rc.used_at = get_now()
                            if rc.has_warranty:
                                days = rc.warranty_days or 30
                                rc.warranty_expires_at = get_now() + timedelta(days=days)

                            record = RedemptionRecord(
                                email=email,
                                code=code,
                                team_id=team_id_final,
                                account_id=target_team.account_id,
                                is_warranty_redemption=rc.has_warranty
                            )
                            db_session.add(record)
                            target_team.current_members += 1
                            if target_team.current_members >= target_team.max_members:
                                target_team.status = "full"

                    # 事务完成提交
                    logger.info(f"兑换核心步骤执行成功: {email} -> Team {team_id_final}")
                    
                    # 5. 后置异步任务 (循环检测 3 次，确保 API 数据同步)
                    is_verified = False
                    for i in range(3):
                        await asyncio.sleep(5)
                        sync_res = await self.team_service.sync_team_info(team_id_final, db_session)
                        member_emails = [m.lower() for m in sync_res.get("member_emails", [])]
                        if email.lower() in member_emails:
                            is_verified = True
                            logger.info(f"Team {team_id_final} 同步确认成功 (尝试第 {i+1} 次)")
                            break
                        if i < 2:
                            logger.warning(f"Team {team_id_final} 同步尚未见到成员 {email}，准备第 {i+2} 次重试...")
                    
                    if not is_verified:
                        logger.error(f"检测到“虚假成功”: Team {team_id_final} 兑换成功但 3 次后仍查不到成员 {email}")
                        # 获取 Team 对象并通过 _handle_api_error 标记为异常
                        stmt = select(Team).where(Team.id == team_id_final)
                        t_res = await db_session.execute(stmt)
                        target_t = t_res.scalar_one_or_none()
                        if target_t:
                            await self.team_service._handle_api_error(
                                {"success": False, "error": "兑换成功但 3 次同步列表均未见成员", "error_code": "ghost_success"},
                                target_t, db_session
                            )
                    
                    try:
                        asyncio.create_task(notification_service.check_and_notify_low_stock())
                    except:
                        pass
                    
                    return {
                        "success": True,
                        "message": "兑换成功！邀请链接已发送至您的邮箱，请及时查收。",
                        "team_info": {
                            "id": team_id_final,
                            "name": target_team.team_name if target_team else f"Team {team_id_final}",
                            "email": target_team.email if target_team else ""
                        }
                    }

                except Exception as e:
                    last_error = str(e)
                    logger.error(f"兑换迭代失败 ({attempt+1}): {last_error}")
                    
                    try:
                        if db_session.in_transaction():
                            await db_session.rollback()
                    except:
                        pass
                    
                    # 判读是否中断重试
                    if any(kw in last_error for kw in ["不存在", "已使用", "已有正在使用", "质保已过期"]):
                        return {"success": False, "error": last_error}

                    # 判定是否需要永久标记为“满员” (独立事务中执行，防止被主流程回滚)
                    if any(kw in last_error.lower() for kw in ["已满", "seats", "full"]):
                        try:
                            # 重新查询并更新，确保不在主事务回滚的影响下
                            if not team_id:
                                # 如果是自动选择的 ID 报错了，我们去更新数据库中的那个 ID
                                from sqlalchemy import update as sqlalchemy_update
                                await db_session.execute(
                                    sqlalchemy_update(Team).where(Team.id == team_id_final).values(status="full")
                                )
                                await db_session.commit()
                                logger.info(f"已在独立提交中将 Team {team_id_final} 标记为 full")
                            current_target_team_id = None
                        except:
                            pass
                    
                    if attempt < max_retries - 1:
                        await asyncio.sleep(1.5 * (attempt + 1))
                        continue

            return {
                "success": False,
                "error": f"兑换失败次数过多。最后报错: {last_error}"
            }

# 创建全局实例
redeem_flow_service = RedeemFlowService()
