package coordinator

import (
	"sort"

	"github.com/michael-emmanuel/eventstream/internal/protocol"
)

// Assignor names.
const (
	AssignorRange      = "range"
	AssignorRoundRobin = "roundrobin"
	AssignorSticky     = "sticky"
)

type tp = protocol.TopicPartition

type assignInput struct {
	members    []string            // sorted member IDs
	subs       map[string][]string // member -> topics
	partitions map[string]int      // topic -> partition count
	prev       map[string][]tp     // previous assignment (for sticky)
}

func validAssignor(name string) bool {
	return name == AssignorRange || name == AssignorRoundRobin || name == AssignorSticky
}

// assign computes an assignment in which every partition of a subscribed
// topic has exactly one owner. All assignors are deterministic.
func assign(name string, in assignInput) map[string][]tp {
	out := make(map[string][]tp, len(in.members))
	for _, m := range in.members {
		out[m] = nil
	}
	if len(in.members) == 0 {
		return out
	}
	switch name {
	case AssignorRoundRobin:
		roundRobin(in, out)
	case AssignorSticky:
		if identicalSubscriptions(in) {
			sticky(in, out)
		} else {
			roundRobin(in, out)
		}
	default:
		rangeAssign(in, out)
	}
	for m := range out {
		sortTPs(out[m])
	}
	return out
}

func subscribed(in assignInput, m, topic string) bool {
	for _, t := range in.subs[m] {
		if t == topic {
			return true
		}
	}
	return false
}

func topicsOf(in assignInput) []string {
	set := map[string]bool{}
	for _, ts := range in.subs {
		for _, t := range ts {
			if in.partitions[t] > 0 {
				set[t] = true
			}
		}
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

func allTPs(in assignInput) []tp {
	var out []tp
	for _, t := range topicsOf(in) {
		for p := 0; p < in.partitions[t]; p++ {
			out = append(out, tp{Topic: t, Partition: int32(p)})
		}
	}
	return out
}

// rangeAssign gives each consumer a contiguous range of each topic's
// partitions. Simple and predictable; uneven when partition counts are not
// multiples of the consumer count, and the unevenness compounds across
// topics (the first consumers get the extras every time).
func rangeAssign(in assignInput, out map[string][]tp) {
	for _, t := range topicsOf(in) {
		var cs []string
		for _, m := range in.members {
			if subscribed(in, m, t) {
				cs = append(cs, m)
			}
		}
		n, k := in.partitions[t], len(cs)
		per, extra := n/k, n%k
		p := 0
		for i, m := range cs {
			cnt := per
			if i < extra {
				cnt++
			}
			for j := 0; j < cnt; j++ {
				out[m] = append(out[m], tp{Topic: t, Partition: int32(p)})
				p++
			}
		}
	}
}

// roundRobin deals partitions across consumers one at a time.
func roundRobin(in assignInput, out map[string][]tp) {
	i := 0
	for _, x := range allTPs(in) {
		for tries := 0; tries < len(in.members); tries++ {
			m := in.members[i%len(in.members)]
			i++
			if subscribed(in, m, x.Topic) {
				out[m] = append(out[m], x)
				break
			}
		}
	}
}

func identicalSubscriptions(in assignInput) bool {
	var first []string
	for i, m := range in.members {
		s := append([]string(nil), in.subs[m]...)
		sort.Strings(s)
		if i == 0 {
			first = s
			continue
		}
		if len(s) != len(first) {
			return false
		}
		for j := range s {
			if s[j] != first[j] {
				return false
			}
		}
	}
	return true
}

// sticky produces a balanced assignment that keeps as many partitions as
// possible with their previous owner. Fewer moved partitions means less
// state to rebuild and fewer in-flight records reprocessed after a
// rebalance; it is the first step towards cooperative rebalancing.
func sticky(in assignInput, out map[string][]tp) {
	all := allTPs(in)
	m := len(in.members)
	base, extra := len(all)/m, len(all)%m
	valid := map[tp]bool{}
	for _, x := range all {
		valid[x] = true
	}
	taken := map[tp]bool{}
	order := append([]string(nil), in.members...)
	sort.SliceStable(order, func(i, j int) bool { return len(in.prev[order[i]]) > len(in.prev[order[j]]) })
	for _, mem := range order {
		prev := append([]tp(nil), in.prev[mem]...)
		sortTPs(prev)
		for _, x := range prev {
			if !valid[x] || taken[x] {
				continue
			}
			n := len(out[mem])
			if n < base || (n == base && extra > 0) {
				if n == base {
					extra--
				}
				out[mem] = append(out[mem], x)
				taken[x] = true
			}
		}
	}
	for _, x := range all {
		if taken[x] {
			continue
		}
		best := ""
		for _, mem := range in.members {
			if best == "" || len(out[mem]) < len(out[best]) {
				best = mem
			}
		}
		out[best] = append(out[best], x)
		taken[x] = true
	}
}

func sortTPs(s []tp) {
	sort.Slice(s, func(i, j int) bool {
		if s[i].Topic != s[j].Topic {
			return s[i].Topic < s[j].Topic
		}
		return s[i].Partition < s[j].Partition
	})
}

// moved counts partitions whose owner changed between two assignments.
func moved(prev, next map[string][]tp) int {
	owner := map[tp]string{}
	for m, ps := range prev {
		for _, x := range ps {
			owner[x] = m
		}
	}
	n := 0
	for m, ps := range next {
		for _, x := range ps {
			if o, ok := owner[x]; ok && o != m {
				n++
			}
		}
	}
	return n
}
