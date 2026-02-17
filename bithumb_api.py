"""
빗썸 API 클라이언트 모듈
KRW 마켓 종목 목록 조회, 캔들 데이터 조회 및 거래량 분석
"""
import logging
import random
import time
from typing import Any, Dict, List, Optional

import requests

# 로거 초기화 (모듈 레벨)
logger = logging.getLogger(__name__)


class BithumbAPI:
    """빗썸 공개 API 클라이언트"""

    BASE_URL = "https://api.bithumb.com/public"

    def __init__(self, timeout: int = 10, max_retries: int = 2):
        """
        Args:
            timeout: API 요청 타임아웃 (초 단위, 기본값: 10초)
            max_retries: 실패 시 재시도 횟수 (기본값: 2)
        """
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'BithumbAlertBot/1.0'
        })
        self.timeout = timeout
        self.max_retries = max_retries

    def _request_json(self, url: str, *, params: Optional[Dict[str, Any]] = None, context: str = "") -> Optional[Dict[str, Any]]:
        attempts = self.max_retries + 1

        for attempt in range(1, attempts + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout:
                logger.warning(f"{context} 타임아웃 (timeout={self.timeout}초, attempt={attempt}/{attempts})")
            except requests.exceptions.RequestException as e:
                logger.warning(f"{context} 네트워크 오류 (attempt={attempt}/{attempts}): {e}")
            except Exception as e:
                logger.error(f"{context} 예외 (attempt={attempt}/{attempts}): {e}", exc_info=True)

            if attempt < attempts:
                base_delay = min(3.0, 0.3 * (2 ** (attempt - 1)))
                jitter = random.uniform(0, 0.2)
                time.sleep(base_delay + jitter)

        return None

    def get_krw_markets(self) -> List[str]:
        """
        KRW 마켓 상장 종목 목록 조회

        Returns:
            List[str]: 종목 코드 리스트 (예: ['BTC', 'ETH', 'XRP', ...])
        """
        url = f"{self.BASE_URL}/ticker/ALL_KRW"
        data = self._request_json(url, context="KRW 마켓 목록 조회")

        if not data:
            return []

        if data.get('status') == '0000':
            markets = [code for code in data.get('data', {}).keys() if code != 'date']
            logger.debug(f"KRW 마켓 목록 조회 성공: {len(markets)}개 종목")
            return sorted(markets)

        error_msg = data.get('message', 'Unknown error')
        logger.error(f"빗썸 API 오류 (KRW 마켓 목록): {error_msg}")
        return []

    def get_candlestick(
        self,
        order_currency: str,
        payment_currency: str = "KRW",
        chart_intervals: str = "5m",
        count: int = 100
    ) -> Optional[List[Dict]]:
        """
        캔들 데이터 조회

        Args:
            order_currency: 주문 통화 (예: 'BTC')
            payment_currency: 결제 통화 (기본값: 'KRW')
            chart_intervals: 차트 간격 (기본값: '5m')
            count: 조회할 캔들 개수 (기본값: 100)

        Returns:
            List[Dict]: 캔들 데이터 리스트
        """
        url = f"{self.BASE_URL}/candlestick/{order_currency}_{payment_currency}/{chart_intervals}"
        params = {'count': count}

        data = self._request_json(
            url,
            params=params,
            context=f"{order_currency} 캔들 데이터 조회",
        )

        if not data:
            return None

        if data.get('status') == '0000':
            candles = data.get('data', [])
            parsed_candles = self._parse_candles(candles)
            logger.debug(f"{order_currency} 캔들 데이터 조회 성공: {len(parsed_candles)}개")
            return parsed_candles

        error_msg = data.get('message', 'Unknown error')
        logger.warning(f"캔들 데이터 조회 실패 ({order_currency}): {error_msg}")
        return None

    def _parse_candles(self, raw_data: List) -> List[Dict]:
        """
        빗썸 API 응답 데이터를 표준 형식으로 변환
        """
        parsed = []
        for candle in raw_data:
            if isinstance(candle, list) and len(candle) >= 6:
                parsed.append({
                    'time': candle[0],
                    'open': float(candle[1]),
                    'close': float(candle[2]),
                    'high': float(candle[3]),
                    'low': float(candle[4]),
                    'volume': float(candle[5])
                })
            elif isinstance(candle, dict):
                parsed.append({
                    'time': candle.get('time', candle.get('dt', 0)),
                    'open': float(candle.get('open', candle.get('openPrice', 0))),
                    'close': float(candle.get('close', candle.get('closePrice', 0))),
                    'high': float(candle.get('high', candle.get('highPrice', 0))),
                    'low': float(candle.get('low', candle.get('lowPrice', 0))),
                    'volume': float(candle.get('volume', candle.get('transactions', 0)))
                })

        parsed.sort(key=lambda x: x['time'])
        return parsed

    def get_current_ticker(self, order_currency: str, payment_currency: str = "KRW") -> Optional[Dict]:
        """
        현재 시세 정보 조회 (실시간 거래량 포함)
        """
        url = f"{self.BASE_URL}/ticker/{order_currency}_{payment_currency}"
        data = self._request_json(url, context=f"{order_currency} 시세 조회")

        if not data:
            return None

        if data.get('status') == '0000':
            return data.get('data', {})

        error_msg = data.get('message', 'Unknown error')
        logger.warning(f"시세 조회 실패 ({order_currency}): {error_msg}")
        return None


class VolumeAnalyzer:
    """거래량 분석기"""

    def __init__(self, sma_period: int = 20, volume_multiplier: float = 5.0):
        """
        Args:
            sma_period: 이동평균 계산 기간 (기본값: 20)
            volume_multiplier: 알림을 위한 거래량 배수 (기본값: 5.0)
        """
        self.sma_period = sma_period
        self.volume_multiplier = volume_multiplier

    def calculate_volume_sma(self, candles: List[Dict]) -> Optional[float]:
        """
        거래량의 SMA 계산
        """
        if not candles or len(candles) < self.sma_period:
            return None

        volumes = [candle['volume'] for candle in candles[-self.sma_period:]]
        sma = sum(volumes) / len(volumes)
        return sma

    def check_volume_spike(
        self,
        candles: List[Dict],
        current_volume: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        현재 거래량이 평균 대비 배수 이상인지 확인
        """
        result = {
            'is_spike': False,
            'current_volume': 0.0,
            'sma_volume': 0.0,
            'multiplier': 0.0,
            'candles_needed': self.sma_period
        }

        if not candles:
            return result

        if current_volume is None:
            result['current_volume'] = candles[-1]['volume']
        else:
            result['current_volume'] = current_volume

        sma_volume = self.calculate_volume_sma(candles)
        if sma_volume is None or sma_volume == 0:
            result['candles_needed'] = self.sma_period - len(candles)
            return result

        result['sma_volume'] = sma_volume
        result['multiplier'] = result['current_volume'] / sma_volume
        result['is_spike'] = result['multiplier'] >= self.volume_multiplier

        return result

    def analyze_market(
        self,
        candles: List[Dict],
        symbol: str
    ) -> Optional[Dict]:
        """
        종목의 거래량 스파이크 분석
        """
        if not candles:
            return None

        analysis = self.check_volume_spike(candles)

        if analysis['is_spike']:
            return {
                'symbol': symbol,
                'current_volume': analysis['current_volume'],
                'sma_volume': analysis['sma_volume'],
                'multiplier': analysis['multiplier'],
                'current_price': candles[-1]['close'],
                'timestamp': candles[-1]['time']
            }

        return None
