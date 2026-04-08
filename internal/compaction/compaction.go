package compaction

import (
	api "github.com/akzj/go-fast-kv/internal/compaction/api"
)

// compactionStrategy implements api.CompactionStrategy.
type compactionStrategy struct {
	trigger   api.CompactionTrigger
	selector  api.SegmentSelector
	compactor api.Compactor
	reclaimer api.Reclaimer
	epochMgr  api.EpochManager
}

func (s *compactionStrategy) Trigger()  api.CompactionTrigger  { return s.trigger }
func (s *compactionStrategy) Selector() api.SegmentSelector   { return s.selector }
func (s *compactionStrategy) Compactor() api.Compactor        { return s.compactor }
func (s *compactionStrategy) Reclaimer() api.Reclaimer        { return s.reclaimer }

// NewCompactionStrategy creates a default generational compaction strategy.
func NewCompactionStrategy(config *api.CompactionConfig) api.CompactionStrategy {
	if config == nil {
		config = api.DefaultCompactionConfig()
	}
	epochMgr := NewEpochManager(uint(config.GCThreshold))
	reclaimer := NewReclaimer(epochMgr)
	selector := NewSegmentSelector("age")
	trigger := NewCompactionTrigger(config)
	comp := NewCompactor(nil, reclaimer)
	comp.SetSelector(selector)
	return &compactionStrategy{
		trigger:   trigger,
		selector:  selector,
		compactor: comp,
		reclaimer: reclaimer,
		epochMgr:  epochMgr,
	}
}
