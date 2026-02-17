"""
빗썸 KRW 마켓 거래량 모니터링 메인 스크립트
5분봉 거래량이 20 SMA 대비 5배 이상일 때 디스코드 알림
"""
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Set

from dotenv import load_dotenv

from bithumb_api import BithumbAPI, VolumeAnalyzer
from discord_webhook import DiscordWebhook

# 환경 변수 로드
load_dotenv()

# 로그 파일 경로 설정
log_path = os.getenv('LOG_FILE', 'bithumb_monitor.log')
log_file = Path(log_path)

# 로그 디렉토리가 없으면 생성
log_file.parent.mkdir(parents=True, exist_ok=True)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(str(log_file), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info(f"로그 파일 저장 위치: {log_file.absolute()}")


class BithumbVolumeMonitor:
    """빗썸 거래량 모니터링 클래스"""

    def __init__(
        self,
        webhook_url: str,
        check_interval: int = 300,  # 5분 (초 단위)
        volume_multiplier: float = 5.0,
        sma_period: int = 20,
        candle_interval: str = "5m",
        api_timeout: int = 10,
        webhook_timeout: int = 10,
        api_delay: float = 0.1,
        alert_reset_hours: Optional[int] = None,
        send_startup_test: bool = False,
        alert_cache_file: str = "alerted_symbols_cache.json",
        min_krw_volume: float = 0.0,
        api_max_retries: int = 2,
    ):
        """
        Args:
            webhook_url: 디스코드 웹훅 URL
            check_interval: 체크 간격 (초 단위, 기본값: 300초 = 5분)
            volume_multiplier: 거래량 배수 (기본값: 5.0)
            sma_period: SMA 기간 (기본값: 20)
            candle_interval: 캔들 기간 (기본값: "5m")
            api_timeout: API 요청 타임아웃 (초 단위, 기본값: 10초)
            webhook_timeout: 웹훅 요청 타임아웃 (초 단위, 기본값: 10초)
            api_delay: API 호출 간 딜레이 (초 단위, 기본값: 0.1초)
            alert_reset_hours: 알림 리셋 주기 (시간 단위, None이면 리셋 안 함)
            send_startup_test: 연속 모드 시작 시 테스트 메시지 전송 여부
            alert_cache_file: 알림 이력 캐시 파일 경로
            min_krw_volume: 최소 거래대금(원) 기준. 미만이면 알림 제외
            api_max_retries: API 재시도 횟수
        """
        self.bithumb_api = BithumbAPI(timeout=api_timeout, max_retries=api_max_retries)
        self.volume_analyzer = VolumeAnalyzer(
            sma_period=sma_period,
            volume_multiplier=volume_multiplier
        )
        self.discord_webhook = DiscordWebhook(webhook_url, timeout=webhook_timeout)
        self.check_interval = check_interval
        self.candle_interval = candle_interval
        self.api_delay = api_delay
        self.alert_reset_hours = alert_reset_hours
        self.send_startup_test = send_startup_test
        self.min_krw_volume = min_krw_volume
        self.alerted_symbols: Set[str] = set()  # 이미 알림을 보낸 종목 추적
        self.last_reset_time: Optional[datetime] = None  # 마지막 리셋 시간
        self.alert_cache_path = Path(alert_cache_file)
        self._load_alert_cache()

    def _load_alert_cache(self):
        """파일 캐시에서 이전 알림 이력 복원"""
        try:
            if not self.alert_cache_path.exists():
                return

            data = json.loads(self.alert_cache_path.read_text(encoding='utf-8'))
            if isinstance(data, list):
                self.alerted_symbols = {str(symbol) for symbol in data if symbol}
                logger.info(f"알림 캐시 복원 완료: {len(self.alerted_symbols)}개 종목")
        except Exception as e:
            logger.warning(f"알림 캐시 복원 실패(무시): {e}")

    def _save_alert_cache(self):
        """현재 알림 이력을 파일에 저장"""
        try:
            if self.alert_cache_path.parent != Path('.'):
                self.alert_cache_path.parent.mkdir(parents=True, exist_ok=True)

            payload = sorted(self.alerted_symbols)
            temp_path = self.alert_cache_path.with_suffix(self.alert_cache_path.suffix + '.tmp')
            temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
            temp_path.replace(self.alert_cache_path)
        except Exception as e:
            logger.warning(f"알림 캐시 저장 실패(무시): {e}")

    def get_all_krw_symbols(self) -> list:
        """KRW 마켓 모든 종목 조회"""
        logger.info("KRW 마켓 종목 목록 조회 중...")
        symbols = self.bithumb_api.get_krw_markets()
        logger.info(f"총 {len(symbols)}개 종목 발견")
        return symbols

    def check_symbol_volume(self, symbol: str) -> Optional[dict]:
        """
        특정 종목의 거래량 스파이크 확인

        Args:
            symbol: 종목 코드 (예: 'BTC')

        Returns:
            dict: 분석 결과 (스파이크가 없으면 None)
        """
        try:
            candles = self.bithumb_api.get_candlestick(
                order_currency=symbol,
                payment_currency="KRW",
                chart_intervals=self.candle_interval,
                count=50
            )

            if not candles or len(candles) < 20:
                logger.debug(f"{symbol}: 캔들 데이터 부족 ({len(candles) if candles else 0}개)")
                return None

            analysis = self.volume_analyzer.analyze_market(candles, symbol)
            if not analysis:
                return None

            # 저유동성 필터: 현재 거래량 * 현재가 기준
            notional_krw = float(analysis['current_volume']) * float(analysis['current_price'])
            if self.min_krw_volume > 0 and notional_krw < self.min_krw_volume:
                logger.debug(
                    f"{symbol}: 저유동성 필터로 제외 "
                    f"(거래대금={notional_krw:,.0f} KRW < 최소 {self.min_krw_volume:,.0f} KRW)"
                )
                return None

            analysis['notional_krw'] = notional_krw
            return analysis

        except Exception as e:
            logger.error(f"{symbol} 분석 중 오류: {e}")
            return None

    def _reset_alerted_symbols_if_needed(self):
        """
        설정된 시간이 지나면 알림된 종목 목록 리셋 (메모리 누수 방지)
        """
        if self.alert_reset_hours is None:
            return

        now = datetime.now()

        if self.last_reset_time is None:
            self.last_reset_time = now
            return

        time_diff = now - self.last_reset_time
        if time_diff >= timedelta(hours=self.alert_reset_hours):
            reset_count = len(self.alerted_symbols)
            self.alerted_symbols.clear()
            self.last_reset_time = now
            self._save_alert_cache()
            logger.info(f"알림된 종목 목록 리셋 (리셋된 종목 수: {reset_count}개)")

    def send_alert_if_needed(self, analysis: dict):
        """
        필요시 디스코드 알림 전송

        Args:
            analysis: 분석 결과 딕셔너리
        """
        if not analysis:
            return

        symbol = analysis['symbol']

        self._reset_alerted_symbols_if_needed()

        if symbol in self.alerted_symbols:
            logger.debug(f"{symbol}: 이미 알림 전송됨 (스킵)")
            return

        success = self.discord_webhook.send_alert(analysis, candle_interval=self.candle_interval)

        if success:
            logger.info(
                f"🚨 {symbol} 거래량 급증 알림 전송 성공 "
                f"(배수: {analysis['multiplier']:.2f}배, 거래대금: {analysis.get('notional_krw', 0):,.0f} KRW)"
            )
            self.alerted_symbols.add(symbol)
            self._save_alert_cache()
        else:
            logger.error(f"{symbol} 알림 전송 실패")

    def monitor_once(self):
        """한 번의 모니터링 사이클 실행"""
        logger.info("=" * 60)
        logger.info(f"모니터링 시작 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        symbols = self.get_all_krw_symbols()

        if not symbols:
            logger.warning("종목 목록을 가져올 수 없습니다.")
            return

        spike_count = 0

        for i, symbol in enumerate(symbols, 1):
            logger.debug(f"[{i}/{len(symbols)}] {symbol} 체크 중...")

            analysis = self.check_symbol_volume(symbol)

            if analysis:
                logger.warning(
                    f"⚠️ {symbol} 거래량 급증 감지! "
                    f"현재: {analysis['current_volume']:,.2f}, "
                    f"평균: {analysis['sma_volume']:,.2f}, "
                    f"배수: {analysis['multiplier']:.2f}배"
                )
                self.send_alert_if_needed(analysis)
                spike_count += 1

            time.sleep(self.api_delay)

        if spike_count > 0:
            logger.info(f"모니터링 완료 - 총 {len(symbols)}개 종목 체크, {spike_count}개 거래량 급증 발견")
        else:
            logger.debug(f"모니터링 완료 - {len(symbols)}개 종목 이상 없음")

    def run_continuous(self):
        """지속적으로 모니터링 실행"""
        logger.info("빗썸 거래량 모니터링 시작")
        logger.info(f"체크 간격: {self.check_interval}초 ({self.check_interval / 60:.1f}분)")
        logger.info(f"캔들 기간: {self.candle_interval}")

        if self.send_startup_test:
            self.discord_webhook.send_test_message()

        consecutive_failures = 0

        try:
            while True:
                try:
                    self.monitor_once()
                    consecutive_failures = 0

                    logger.info(f"{self.check_interval}초 후 다음 체크 예정...")
                    time.sleep(self.check_interval)

                except KeyboardInterrupt:
                    logger.info("사용자에 의해 중지되었습니다.")
                    break
                except Exception as e:
                    consecutive_failures += 1
                    logger.error(f"모니터링 중 오류 발생: {e}", exc_info=True)

                    base_backoff = min(300, 30 * (2 ** (consecutive_failures - 1)))
                    jitter = random.uniform(0, min(10, base_backoff * 0.2))
                    wait_seconds = int(base_backoff + jitter)

                    logger.info(
                        f"연속 실패 {consecutive_failures}회 - "
                        f"{wait_seconds}초 후 재시도합니다."
                    )
                    time.sleep(wait_seconds)

        except Exception as e:
            logger.error(f"치명적 오류: {e}", exc_info=True)
        finally:
            self._save_alert_cache()
            logger.info("모니터링 종료")


def validate_config(
    check_interval: int,
    volume_multiplier: float,
    sma_period: int,
    candle_interval: str,
    api_timeout: int,
    webhook_timeout: int,
    api_delay: float,
    alert_reset_hours: Optional[int],
    min_krw_volume: float,
    api_max_retries: int,
) -> bool:
    """
    설정 값 검증

    Returns:
        bool: 검증 통과 여부
    """
    errors = []

    if check_interval < 60:
        errors.append("CHECK_INTERVAL은 최소 60초 이상이어야 합니다.")

    if volume_multiplier <= 0:
        errors.append("VOLUME_MULTIPLIER는 0보다 커야 합니다.")

    if sma_period < 1:
        errors.append("SMA_PERIOD는 최소 1 이상이어야 합니다.")

    valid_intervals = ['1m', '3m', '5m', '15m', '30m', '1h', '4h', '1d']
    if candle_interval not in valid_intervals:
        errors.append(f"CANDLE_INTERVAL은 다음 중 하나여야 합니다: {', '.join(valid_intervals)}")

    if api_timeout < 1:
        errors.append("API_TIMEOUT은 최소 1초 이상이어야 합니다.")

    if webhook_timeout < 1:
        errors.append("WEBHOOK_TIMEOUT은 최소 1초 이상이어야 합니다.")

    if api_delay < 0:
        errors.append("API_DELAY는 0 이상이어야 합니다.")

    if alert_reset_hours is not None and alert_reset_hours < 1:
        errors.append("ALERT_RESET_HOURS는 최소 1시간 이상이어야 합니다.")

    if min_krw_volume < 0:
        errors.append("MIN_KRW_VOLUME은 0 이상이어야 합니다.")

    if api_max_retries < 0:
        errors.append("API_MAX_RETRIES는 0 이상이어야 합니다.")

    if errors:
        for error in errors:
            logger.error(f"설정 오류: {error}")
        return False

    return True


def main():
    """메인 함수"""
    webhook_url = os.getenv('DISCORD_WEBHOOK_URL')

    if not webhook_url:
        logger.error("환경 변수 DISCORD_WEBHOOK_URL이 설정되지 않았습니다.")
        logger.error(".env 파일에 DISCORD_WEBHOOK_URL을 설정해주세요.")
        return

    check_interval = int(os.getenv('CHECK_INTERVAL', '300'))
    volume_multiplier = float(os.getenv('VOLUME_MULTIPLIER', '5.0'))
    sma_period = int(os.getenv('SMA_PERIOD', '20'))
    candle_interval = os.getenv('CANDLE_INTERVAL', '5m')
    api_timeout = int(os.getenv('API_TIMEOUT', '10'))
    webhook_timeout = int(os.getenv('WEBHOOK_TIMEOUT', '10'))
    api_delay = float(os.getenv('API_DELAY', '0.1'))
    alert_reset_hours_str = os.getenv('ALERT_RESET_HOURS', '')
    alert_reset_hours = int(alert_reset_hours_str) if alert_reset_hours_str else None
    send_startup_test = os.getenv('SEND_STARTUP_TEST', 'false').lower() == 'true'
    alert_cache_file = os.getenv('ALERT_CACHE_FILE', 'alerted_symbols_cache.json')
    min_krw_volume = float(os.getenv('MIN_KRW_VOLUME', '0'))
    api_max_retries = int(os.getenv('API_MAX_RETRIES', '2'))

    if not validate_config(
        check_interval,
        volume_multiplier,
        sma_period,
        candle_interval,
        api_timeout,
        webhook_timeout,
        api_delay,
        alert_reset_hours,
        min_krw_volume,
        api_max_retries,
    ):
        logger.error("설정 검증 실패. 프로그램을 종료합니다.")
        return

    monitor = BithumbVolumeMonitor(
        webhook_url=webhook_url,
        check_interval=check_interval,
        volume_multiplier=volume_multiplier,
        sma_period=sma_period,
        candle_interval=candle_interval,
        api_timeout=api_timeout,
        webhook_timeout=webhook_timeout,
        api_delay=api_delay,
        alert_reset_hours=alert_reset_hours,
        send_startup_test=send_startup_test,
        alert_cache_file=alert_cache_file,
        min_krw_volume=min_krw_volume,
        api_max_retries=api_max_retries,
    )

    run_once = os.getenv('RUN_ONCE', 'false').lower() == 'true'

    if run_once:
        logger.info("단일 실행 모드")
        monitor.monitor_once()
    else:
        logger.info("연속 실행 모드")
        monitor.run_continuous()


if __name__ == "__main__":
    main()
