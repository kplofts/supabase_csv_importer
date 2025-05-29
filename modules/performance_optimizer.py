"""
Performance Optimizer Module
Optimizes import settings based on Supabase instance and local resources
"""
import psutil
import multiprocessing
from typing import Dict, Any, Tuple


class PerformanceOptimizer:
    """Optimizes import settings based on instance size and performance level"""
    
    # Supabase instance specifications
    INSTANCE_SPECS = {
        1: {"name": "Nano", "memory_gb": 0.5, "cpu_cores": 1, "tier": "shared"},
        2: {"name": "Micro", "memory_gb": 1, "cpu_cores": 2, "tier": "dedicated"},
        3: {"name": "Small", "memory_gb": 2, "cpu_cores": 2, "tier": "dedicated"},
        4: {"name": "Medium", "memory_gb": 4, "cpu_cores": 2, "tier": "dedicated"},
        5: {"name": "Large", "memory_gb": 8, "cpu_cores": 2, "tier": "dedicated"},
        6: {"name": "XL", "memory_gb": 16, "cpu_cores": 4, "tier": "dedicated"},
        7: {"name": "2XL", "memory_gb": 32, "cpu_cores": 8, "tier": "dedicated"},
        8: {"name": "4XL", "memory_gb": 64, "cpu_cores": 16, "tier": "dedicated"},
        9: {"name": "8XL", "memory_gb": 128, "cpu_cores": 32, "tier": "dedicated"},
        10: {"name": "12XL", "memory_gb": 192, "cpu_cores": 48, "tier": "dedicated"},
        11: {"name": "16XL", "memory_gb": 256, "cpu_cores": 64, "tier": "dedicated"},
    }
    
    # Performance levels
    PERFORMANCE_LEVELS = {
        1: {"name": "Conservative", "multiplier": 0.5},
        2: {"name": "Balanced", "multiplier": 0.75},
        3: {"name": "Aggressive", "multiplier": 1.0},
    }
    
    def __init__(self):
        self.local_specs = self._get_local_specs()
    
    def _get_local_specs(self) -> Dict[str, Any]:
        """Get local computer specifications"""
        return {
            "cpu_cores": multiprocessing.cpu_count(),
            "memory_gb": psutil.virtual_memory().total / (1024**3),
            "available_memory_gb": psutil.virtual_memory().available / (1024**3),
            "cpu_percent": psutil.cpu_percent(interval=1)
        }
    
    def get_optimized_settings(self, instance_size: int, performance_level: int) -> Dict[str, Any]:
        """Get optimized settings based on instance size and performance level"""
        
        # Validate inputs
        if instance_size not in self.INSTANCE_SPECS:
            raise ValueError(f"Invalid instance size. Choose between 1-{len(self.INSTANCE_SPECS)}")
        if performance_level not in self.PERFORMANCE_LEVELS:
            raise ValueError("Invalid performance level. Choose 1 (Conservative), 2 (Balanced), or 3 (Aggressive)")
        
        instance = self.INSTANCE_SPECS[instance_size]
        perf = self.PERFORMANCE_LEVELS[performance_level]
        
        # Calculate optimal settings
        settings = self._calculate_settings(instance, perf)
        
        return settings
    
    def _calculate_settings(self, instance: Dict[str, Any], perf: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate optimal settings based on instance and performance level"""
        
        multiplier = perf["multiplier"]
        
        # Base calculations on instance size
        instance_memory = instance["memory_gb"]
        instance_cores = instance["cpu_cores"]
        
        # Consider local computer limitations
        local_memory = self.local_specs["available_memory_gb"]
        local_cores = self.local_specs["cpu_cores"]
        
        # Connection pool settings
        if instance["tier"] == "shared":
            # Nano instance has limited connections
            max_connections = int(5 * multiplier)
            parallel_workers = 1
        else:
            # Calculate based on instance size
            max_connections = min(
                int(10 + (instance_cores * 2) * multiplier),
                50  # Supabase connection limit
            )
            parallel_workers = min(
                int(instance_cores * multiplier),
                local_cores - 1,  # Leave one core for system
                max_connections - 2  # Leave connections for main thread
            )
        
        # Memory settings (in MB)
        work_mem = self._calculate_work_mem(instance_memory, multiplier)
        maintenance_work_mem = work_mem * 4
        
        # Chunk size based on available memory
        chunk_size_mb = self._calculate_chunk_size(
            instance_memory, 
            local_memory, 
            multiplier
        )
        
        # Batch size based on memory and performance level
        batch_size = self._calculate_batch_size(instance_memory, multiplier)
        
        # Statement timeout based on chunk size and performance
        timeout = self._calculate_timeout(chunk_size_mb, multiplier)
        
        return {
            "database": {
                "pool": {
                    "min_connections": max(2, parallel_workers),
                    "max_connections": max_connections,
                    "keepalive": 30
                }
            },
            "import": {
                "chunk_size_mb": chunk_size_mb,
                "batch_size": batch_size,
                "parallel_workers": parallel_workers,
                "use_copy": True
            },
            "optimization": {
                "work_mem": f"{work_mem}MB",
                "maintenance_work_mem": f"{maintenance_work_mem}MB",
                "statement_timeout": timeout,
                "disable_triggers": multiplier > 0.5,  # Only for balanced/aggressive
                "run_vacuum": multiplier < 1.0,  # Skip for aggressive
                "run_analyze": True
            },
            "recommended_actions": self._get_recommendations(instance, perf)
        }
    
    def _calculate_work_mem(self, instance_memory: float, multiplier: float) -> int:
        """Calculate optimal work_mem setting"""
        # Base calculation: 5-10% of instance memory
        base_percent = 0.05 + (0.05 * multiplier)
        work_mem = int(instance_memory * 1024 * base_percent)
        
        # Limits
        return max(64, min(work_mem, 1024))  # 64MB-1GB range
    
    def _calculate_chunk_size(self, instance_memory: float, local_memory: float, 
                             multiplier: float) -> int:
        """Calculate optimal chunk size"""
        # Consider both instance and local memory
        limiting_memory = min(instance_memory, local_memory * 0.5)  # Use 50% of local
        
        # Base calculation
        if limiting_memory < 2:  # Small instances
            chunk_size = int(50 + (50 * multiplier))
        elif limiting_memory < 8:  # Medium instances
            chunk_size = int(100 + (100 * multiplier))
        else:  # Large instances
            chunk_size = int(200 + (300 * multiplier))
        
        return min(chunk_size, 1000)  # Cap at 1GB chunks
    
    def _calculate_batch_size(self, instance_memory: float, multiplier: float) -> int:
        """Calculate optimal batch size for INSERT operations"""
        if instance_memory < 2:
            base = 1000
        elif instance_memory < 8:
            base = 5000
        else:
            base = 10000
        
        return int(base * (1 + multiplier))
    
    def _calculate_timeout(self, chunk_size_mb: int, multiplier: float) -> str:
        """Calculate appropriate statement timeout"""
        # Estimate: 1 minute per 50MB at conservative, less for aggressive
        minutes = int((chunk_size_mb / 50) * (2 - multiplier) * 10)
        
        if minutes <= 30:
            return "30min"
        elif minutes <= 60:
            return "1h"
        else:
            return "2h"
    
    def _get_recommendations(self, instance: Dict[str, Any], perf: Dict[str, Any]) -> list:
        """Get performance recommendations"""
        recommendations = []
        
        # Instance-specific recommendations
        if instance["tier"] == "shared":
            recommendations.append("⚠️  Nano instance has limited resources. Consider upgrading for better performance.")
            recommendations.append("💡 Import during off-peak hours for better performance.")
        
        if instance["memory_gb"] < 4:
            recommendations.append("💡 Consider processing files sequentially rather than in parallel.")
        
        # Performance level recommendations
        if perf["multiplier"] == 1.0:
            recommendations.append("🚀 Aggressive mode: Monitor for connection limits and timeouts.")
            recommendations.append("⚡ Disable non-critical indexes before import for maximum speed.")
        elif perf["multiplier"] == 0.5:
            recommendations.append("🛡️  Conservative mode: Stable but slower. Good for production imports.")
        
        # Local computer recommendations
        if self.local_specs["available_memory_gb"] < 4:
            recommendations.append("💻 Local memory is limited. Close other applications for better performance.")
        
        if self.local_specs["cpu_percent"] > 80:
            recommendations.append("💻 CPU usage is high. Consider reducing parallel workers.")
        
        return recommendations
    
    def print_optimization_summary(self, settings: Dict[str, Any], instance_size: int, 
                                  performance_level: int) -> None:
        """Print a summary of optimization settings"""
        instance = self.INSTANCE_SPECS[instance_size]
        perf = self.PERFORMANCE_LEVELS[performance_level]
        
        print("\n" + "="*60)
        print("🔧 PERFORMANCE OPTIMIZATION SUMMARY")
        print("="*60)
        print(f"Instance: {instance['name']} ({instance['memory_gb']}GB RAM, {instance['cpu_cores']} cores)")
        print(f"Performance: {perf['name']}")
        print(f"Local Computer: {self.local_specs['cpu_cores']} cores, {self.local_specs['memory_gb']:.1f}GB RAM")
        print("\n📊 Optimized Settings:")
        print(f"  • Chunk Size: {settings['import']['chunk_size_mb']}MB")
        print(f"  • Parallel Workers: {settings['import']['parallel_workers']}")
        print(f"  • Connection Pool: {settings['database']['pool']['max_connections']} connections")
        print(f"  • Work Memory: {settings['optimization']['work_mem']}")
        print(f"  • Batch Size: {settings['import']['batch_size']:,} rows")
        print(f"  • Timeout: {settings['optimization']['statement_timeout']}")
        
        if settings['recommended_actions']:
            print("\n💡 Recommendations:")
            for rec in settings['recommended_actions']:
                print(f"  {rec}")
        
        print("="*60)