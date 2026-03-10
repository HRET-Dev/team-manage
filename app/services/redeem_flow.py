"""
兑换流程服务 (Redeem Flow Service)
协调兑换码验证, Team 选择和加入 Team 的完整流程
"""
import logging
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from sqlalchemy import select, update, delete, func
from sqlalchemy.ext.asyncio import AsyncSession
import traceback

from app.models import RedemptionCode, RedemptionRecord, Team
from app.services.redemption import RedemptionService
from app.services.team import TeamService
from app.services.warranty import warranty_service
from app.services.notification import notification_service
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)


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
            # validate_code 内部包含 SELECT, 可能包含状态更新(naive)
            validate_result = await self.redemption_service.validate_code(code, db_session)

            if not validate_result["success"]:
                return {
                    "success": False,
                    "valid": False,
                    "reason": None,
                    "teams": [],
                    "error": validate_result["error"]
                }
            
            # 如果是已经标记为过期了（validate_code 自动触发了过期状态检测并更新了对象）
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

            logger.info(f"自动选择 Team: {team.id} (过期时间: {team.expires_at})")

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

        for attempt in range(max_retries):
            logger.info(f"兑换尝试 {attempt + 1}/{max_retries} (Code: {code}, Email: {email}, Manual Team: {team_id})")
            
            try:
                # 0. 清理先前可能的遗留状态
                if db_session.in_transaction():
                    await db_session.rollback()

                # 1. 验证和初步检查
                # 使用 select ... with_for_update 锁定，防止并发重兑
                stmt = select(RedemptionCode).where(RedemptionCode.code == code).with_for_update()
                res = await db_session.execute(stmt)
                rc = res.scalar_one_or_none()

                if not rc:
                    return {"success": False, "error": "兑换码不存在"}
                
                # 检查状态
                if rc.status not in ["unused", "warranty_active"]:
                    if rc.status == "used":
                        # 检查是否为质保期重复使用
                        warranty_check = await self.warranty_service.validate_warranty_reuse(
                            db_session, code, email
                        )
                        if not warranty_check.get("can_reuse"):
                            return {"success": False, "error": warranty_check.get("reason") or "兑换码已使用"}
                        logger.info(f"验证通过: 允许质保重复兑换 ({email})")
                    else:
                        return {"success": False, "error": f"兑换码状态无效: {rc.status}"}

                # 2. 确定目标 Team
                team_id_final = current_target_team_id
                if not team_id_final:
                    select_res = await self.select_team_auto(db_session)
                    if not select_res["success"]:
                        return {"success": False, "error": select_res["error"]}
                    team_id_final = select_res["team_id"]

                # 3. 执行核心兑换逻辑 (事务保证原子性)
                target_team = None
                try:
                    async with db_session.begin():
                        # A. 再次锁定以确保安全
                        stmt = select(Team).where(Team.id == team_id_final).with_for_update()
                        res = await db_session.execute(stmt)
                        target_team = res.scalar_one_or_none()
                        
                        if not target_team or target_team.status != "active":
                            raise Exception(f"目标 Team {team_id_final} 不可用")
                        
                        if target_team.current_members >= target_team.max_members:
                            target_team.status = "full"
                            raise Exception("该 Team 已满, 请选择其他 Team 尝试")

                        # B. 获取并验证 Access Token
                        access_token = await self.team_service.ensure_access_token(target_team, db_session)
                        if not access_token:
                            raise Exception("获取 Team 访问权限失败，账户状态异常")

                        # C. 发送 ChatGPT API 邀请
                        invite_res = await self.chatgpt_service.send_invite(
                            access_token, target_team.account_id, email, db_session,
                            identifier=target_team.email
                        )
                        
                        if not invite_res["success"]:
                            err = invite_res.get("error", "邀请失败")
                            if any(kw in str(err).lower() for kw in ["maximum number of seats", "full"]):
                                target_team.status = "full"
                                raise Exception("该 Team 席位已达到上限，请重试")
                            raise Exception(err)

                        # D. 更新本地数据库状态
                        # 重新获取锁 (由于 ensure_access_token 内部可能有提交)
                        rc_stmt = select(RedemptionCode).where(RedemptionCode.code == code).with_for_update()
                        rc_res = await db_session.execute(rc_stmt)
                        rc_obj = rc_res.scalar_one()
                        
                        rc_obj.status = "used"
                        rc_obj.used_by_email = email
                        rc_obj.used_team_id = team_id_final
                        rc_obj.used_at = get_now()
                        if rc_obj.has_warranty:
                            # 提前计算过期时间
                            days = rc_obj.warranty_days or 30
                            rc_obj.warranty_expires_at = get_now() + timedelta(days=days)

                        # 创建记录
                        record = RedemptionRecord(
                            email=email,
                            code=code,
                            team_id=team_id_final,
                            account_id=target_team.account_id,
                            is_warranty_redemption=rc_obj.has_warranty
                        )
                        db_session.add(record)
                        
                        # 同步更新 Team 成员计数
                        target_team.current_members += 1
                        if target_team.current_members >= target_team.max_members:
                            target_team.status = "full"
                    
                    # 提交成功
                    logger.info(f"兑换成功: {email} -> Team {team_id_final}")
                
                    # 4. 后置确认 (异步同步或短时等待同步)
                    await asyncio.sleep(2)
                    sync_res = await self.team_service.sync_team_info(team_id_final, db_session)
                    
                    # 5. 库存通知
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

                except Exception as biz_e:
                    last_error = str(biz_e)
                    logger.warning(f"核心逻辑出错: {biz_e}")
                    if team_id and any(kw in last_error for kw in ["已满", "上限", "不可用"]):
                        return {"success": False, "error": last_error}
                    raise biz_e

            except Exception as e:
                last_error = str(e)
                logger.error(f"兑换迭代失败 ({attempt+1}): {e}")
                
                if db_session.in_transaction():
                    await db_session.rollback()
                
                # 确定不可重试的错误
                if any(kw in last_error for kw in ["不存在", "已使用", "已有正在使用", "质保已过期"]):
                    return {"success": False, "error": last_error}

                if not team_id and ("已满" in last_error or "上限" in last_error or "seats" in last_error.lower()):
                    current_target_team_id = None # 下次尝试自动选别的
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue

        return {
            "success": False,
            "error": f"兑换失败次数过多。最后报错: {last_error}"
        }

    async def _rollback_redemption(self, email: str, code: str, team_id: int, db_session: AsyncSession):
        """简单的回滚清理逻辑"""
        try:
            if db_session.in_transaction():
                await db_session.rollback()
            
            async with db_session.begin():
                # 删除记录
                stmt = delete(RedemptionRecord).where(
                    RedemptionRecord.code == code,
                    RedemptionRecord.email == email
                )
                await db_session.execute(stmt)
                
                # 恢复码状态
                stmt = select(RedemptionCode).where(RedemptionCode.code == code).with_for_update()
                res = await db_session.execute(stmt)
                rc = res.scalar_one_or_none()
                if rc:
                    rc.status = "unused"
                    rc.used_by_email = None
                    rc.used_team_id = None
                    rc.used_at = None
                    if rc.has_warranty:
                        rc.warranty_expires_at = None
            logger.info(f"回滚兑换记录完成: {email}, {code}")
        except Exception as e:
            logger.error(f"回滚兑换记录失败: {e}")

# 创建全局实例
redeem_flow_service = RedeemFlowService()
