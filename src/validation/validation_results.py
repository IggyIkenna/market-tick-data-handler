"""
Validation Results and Reporting

Defines data structures for validation results and comprehensive reporting.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any, Optional
from enum import Enum
import pandas as pd


class ValidationStatus(Enum):
    """Validation status enumeration"""
    PASS = "PASS"
    FAIL = "FAIL"
    WARNING = "WARNING"
    SKIP = "SKIP"


@dataclass
class ValidationResult:
    """Individual validation result"""
    test_name: str
    status: ValidationStatus
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    execution_time_ms: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'test_name': self.test_name,
            'status': self.status.value,
            'message': self.message,
            'details': self.details,
            'timestamp': self.timestamp.isoformat(),
            'execution_time_ms': self.execution_time_ms
        }


@dataclass
class ValidationReport:
    """Comprehensive validation report"""
    report_id: str
    start_time: datetime
    end_time: datetime
    total_tests: int = 0
    passed_tests: int = 0
    failed_tests: int = 0
    warning_tests: int = 0
    skipped_tests: int = 0
    results: List[ValidationResult] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    
    def add_result(self, result: ValidationResult):
        """Add a validation result"""
        self.results.append(result)
        self.total_tests += 1
        
        if result.status == ValidationStatus.PASS:
            self.passed_tests += 1
        elif result.status == ValidationStatus.FAIL:
            self.failed_tests += 1
        elif result.status == ValidationStatus.WARNING:
            self.warning_tests += 1
        elif result.status == ValidationStatus.SKIP:
            self.skipped_tests += 1
    
    def get_success_rate(self) -> float:
        """Calculate success rate as percentage"""
        if self.total_tests == 0:
            return 0.0
        return (self.passed_tests / self.total_tests) * 100.0
    
    def get_status(self) -> ValidationStatus:
        """Get overall validation status"""
        if self.failed_tests > 0:
            return ValidationStatus.FAIL
        elif self.warning_tests > 0:
            return ValidationStatus.WARNING
        elif self.passed_tests > 0:
            return ValidationStatus.PASS
        else:
            return ValidationStatus.SKIP
    
    def to_dataframe(self) -> pd.DataFrame:
        """Convert results to pandas DataFrame"""
        if not self.results:
            return pd.DataFrame()
        
        data = [result.to_dict() for result in self.results]
        return pd.DataFrame(data)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'report_id': self.report_id,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat(),
            'total_tests': self.total_tests,
            'passed_tests': self.passed_tests,
            'failed_tests': self.failed_tests,
            'warning_tests': self.warning_tests,
            'skipped_tests': self.skipped_tests,
            'success_rate': self.get_success_rate(),
            'overall_status': self.get_status().value,
            'summary': self.summary,
            'results': [result.to_dict() for result in self.results]
        }
    
    def print_summary(self):
        """Print a formatted summary"""
        print("\n" + "="*60)
        print("📊 VALIDATION REPORT SUMMARY")
        print("="*60)
        print(f"Report ID: {self.report_id}")
        print(f"Duration: {(self.end_time - self.start_time).total_seconds():.2f}s")
        print(f"Overall Status: {self.get_status().value}")
        print(f"Success Rate: {self.get_success_rate():.1f}%")
        print(f"Total Tests: {self.total_tests}")
        print(f"  ✅ Passed: {self.passed_tests}")
        print(f"  ❌ Failed: {self.failed_tests}")
        print(f"  ⚠️  Warnings: {self.warning_tests}")
        print(f"  ⏭️  Skipped: {self.skipped_tests}")
        
        if self.failed_tests > 0:
            print("\n❌ FAILED TESTS:")
            for result in self.results:
                if result.status == ValidationStatus.FAIL:
                    print(f"  - {result.test_name}: {result.message}")
        
        if self.warning_tests > 0:
            print("\n⚠️  WARNINGS:")
            for result in self.results:
                if result.status == ValidationStatus.WARNING:
                    print(f"  - {result.test_name}: {result.message}")
        
        print("="*60)