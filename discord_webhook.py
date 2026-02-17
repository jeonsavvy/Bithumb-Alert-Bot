"""
디스코드 웹훅 알림 모듈
"""
import requests
import logging
from typing import Dict, Optional
from datetime import datetime

# 로거 초기화 (모듈 레벨)
logger = logging.getLogger(__name__)


class DiscordWebhook:
    """디스코드 웹훅 클라이언트"""
    
    def __init__(self, webhook_url: str, timeout: int = 10):
        """
        Args:
            webhook_url: 디스코드 웹훅 URL
            timeout: 웹훅 요청 타임아웃 (초 단위, 기본값: 10초)
        """
        self.webhook_url = webhook_url
        self.timeout = timeout
        self.session = requests.Session()
    
    def format_price(self, price: float) -> str:
        """
        가격을 적절한 소수점 자릿수로 포맷팅
        
        Args:
            price: 가격
            
        Returns:
            str: 포맷팅된 가격 문자열
        """
        if price == 0:
            return "0 KRW"
        
        # 가격에 따라 소수점 자릿수 결정
        if price < 0.01:
            # 0.01원 미만: 소수점 6자리
            return f"{price:.6f} KRW"
        elif price < 1:
            # 1원 미만: 소수점 4자리
            return f"{price:.4f} KRW"
        elif price < 100:
            # 100원 미만: 소수점 2자리
            return f"{price:.2f} KRW"
        elif price < 1000:
            # 1000원 미만: 소수점 1자리
            return f"{price:,.1f} KRW"
        else:
            # 1000원 이상: 소수점 없음 (천단위 구분자 포함)
            return f"{price:,.0f} KRW"
    
    def send_alert(self, analysis_result: Dict, webhook_url: Optional[str] = None, candle_interval: str = "5m") -> bool:
        """
        거래량 스파이크 알림 전송
        
        Args:
            analysis_result: 분석 결과 딕셔너리
                {
                    'symbol': str,  # 종목 코드
                    'current_volume': float,  # 현재 거래량
                    'sma_volume': float,  # 20 SMA 거래량
                    'multiplier': float,  # 배수
                    'current_price': float,  # 현재 가격
                    'timestamp': int  # 타임스탬프
                }
            webhook_url: 웹훅 URL (None이면 초기화 시 사용한 URL 사용)
            candle_interval: 캔들 기간 (기본값: "5m")
            
        Returns:
            bool: 전송 성공 여부
        """
        url = webhook_url or self.webhook_url
        
        if not url:
            logger.error("웹훅 URL이 설정되지 않았습니다.")
            return False
        
        # 타임스탬프를 읽기 쉬운 형식으로 변환
        timestamp = analysis_result.get('timestamp', 0)
        if timestamp > 0:
            dt = datetime.fromtimestamp(timestamp / 1000)  # 밀리초 단위 가정
            time_str = dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 디스코드 임베드 메시지 생성
        embed = {
            "title": "🚨 거래량 급증 알림",
            "description": f"**{analysis_result['symbol']}/KRW** ({candle_interval})",
            "color": 15158332,  # 빨간색
            "fields": [
                {
                    "name": "현재 거래량",
                    "value": f"{analysis_result['current_volume']:,.2f}",
                    "inline": True
                },
                {
                    "name": "평균 거래량 (20 SMA)",
                    "value": f"{analysis_result['sma_volume']:,.2f}",
                    "inline": True
                },
                {
                    "name": "배수",
                    "value": f"**{analysis_result['multiplier']:.2f}배**",
                    "inline": True
                },
                {
                    "name": "현재 가격",
                    "value": self.format_price(analysis_result['current_price']),
                    "inline": True
                },
                {
                    "name": "시간",
                    "value": time_str,
                    "inline": True
                }
            ],
            "footer": {
                "text": "빗썸얼러트봇"
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
        payload = {
            "embeds": [embed]
        }
        
        try:
            response = self.session.post(
                url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=self.timeout
            )
            response.raise_for_status()
            logger.debug(f"디스코드 알림 전송 성공: {analysis_result.get('symbol', 'Unknown')}")
            return True
        except requests.exceptions.Timeout:
            logger.error(f"디스코드 알림 전송 타임아웃 (timeout={self.timeout}초)")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"디스코드 알림 전송 네트워크 오류: {e}")
            return False
        except Exception as e:
            logger.error(f"디스코드 알림 전송 실패: {e}", exc_info=True)
            return False
    
    def send_test_message(self, webhook_url: Optional[str] = None) -> bool:
        """
        테스트 메시지 전송
        
        Args:
            webhook_url: 웹훅 URL (None이면 초기화 시 사용한 URL 사용)
            
        Returns:
            bool: 전송 성공 여부
        """
        url = webhook_url or self.webhook_url
        
        if not url:
            logger.error("웹훅 URL이 설정되지 않았습니다.")
            return False
        
        payload = {
            "content": "✅ 빗썸얼러트봇이 시작되었습니다!"
        }
        
        try:
            response = self.session.post(
                url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=self.timeout
            )
            response.raise_for_status()
            logger.info("디스코드 테스트 메시지 전송 성공")
            return True
        except requests.exceptions.Timeout:
            logger.error(f"디스코드 테스트 메시지 전송 타임아웃 (timeout={self.timeout}초)")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"디스코드 테스트 메시지 전송 네트워크 오류: {e}")
            return False
        except Exception as e:
            logger.error(f"테스트 메시지 전송 실패: {e}", exc_info=True)
            return False
