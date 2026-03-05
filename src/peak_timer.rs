//! 구간별 duration의 peak(최대값)를 추적하는 타이머 유틸리티.
//!
//! # 예시
//! ```ignore
//! let mut timer = DurationPeakLogger::new();
//! loop {
//!     timer.start();
//!     // ... 작업 수행 ...
//!     timer.stop();
//!     let peak = timer.get_peak();
//! }
//! ```

use chrono::{DateTime, Duration, Utc};

/// 구간별 duration의 peak(최대값)를 추적하는 타이머 객체.
#[derive(Debug, Clone)]
pub struct PeakTimer {
    stime: Option<DateTime<Utc>>,

    peak: Option<Duration>,
    peak_stime: Option<DateTime<Utc>>,
}

impl PeakTimer {
    /// 새 타이머를 생성합니다.
    pub fn new() -> Self {
        Self {
            peak: None,
            peak_stime: None,
            stime: None,
        }
    }

    /// 측정을 시작합니다. `stop()` 호출 전까지의 구간 duration이 기록됩니다.
    pub fn start(&mut self) {
        self.stime = Some(Utc::now());
    }

    /// 측정을 종료하고 duration을 계산합니다. `start()` 이후 경과 시간이 peak보다 크면 peak를 갱신합니다.
    pub fn stop(&mut self) -> Option<Duration> {
        if let Some(stime) = self.stime.take() {
            let duration = Utc::now() - stime;
            if let Some(peak) = self.peak {
                if duration > peak {
                    self.peak = Some(duration);
                }
            } else {
                self.peak = Some(duration);
                self.peak_stime = Some(stime);
            }
            Some(duration)
        } else {
            None
        }
    }

    pub fn reset(&mut self) {
        self.peak = None;
        self.peak_stime = None;
        self.stime = None;
    }

    /// 지금까지 측정된 최대 duration(peak)을 반환합니다.
    pub fn peak(&self) -> Option<Duration> {
        self.peak
    }

    /// peak가 발생한 시점부터 현재까지의 경과 시간을 반환합니다.
    pub fn peak_age(&self) -> Option<Duration> {
        if let Some(peak_stime) = self.peak_stime {
            Some(Utc::now() - peak_stime)
        } else {
            None
        }
    }
}
