package model

import (
	"fmt"
	"strconv"
	"strings"
)

type GTID map[string][]*RangeGTID

func ParseGTID(pos map[string][]string) *GTID {
	m := make(map[string][]*RangeGTID, len(pos))
	for k, v := range pos {
		m[k] = make([]*RangeGTID, len(v))
		for i, str := range v {
			split := strings.Split(str, "-")
			start, _ := strconv.ParseInt(split[0], 10, 64)
			end, _ := strconv.ParseInt(split[1], 10, 64)

			r := &RangeGTID{
				Start: start,
				End:   end,
			}
			m[k][i] = r
		}
	}
	gtid := GTID(m)
	return &gtid
}

func (gtid *GTID) ToMap() map[string]string {
	m := make(map[string]string, len(*gtid))
	for k, v := range *gtid {
		m[k] = k
		for _, r := range v {
			m[k] += fmt.Sprintf(":%d-%d", r.Start, r.End)
		}
	}
	return m
}

func (gtid *GTID) String() string {
	var sb strings.Builder
	for k, v := range *gtid {
		sb.WriteString(k)
		sb.WriteString(":")
		for i, r := range v {
			sb.WriteString(fmt.Sprintf("%d-%d", r.Start, r.End))
			if i < len(v)-1 {
				sb.WriteString(",")
			}
		}
		sb.WriteString(",")
	}
	return sb.String()[:sb.Len()-1]
}

func (gtid *GTID) SetGTID(uuid string, gno int64) {
	m := *gtid
	rangeGTIDS, ok := m[uuid]
	if !ok {
		m[uuid] = []*RangeGTID{{Start: gno, End: gno}}
		return
	}
	length := len(rangeGTIDS)
	rangeGTID := rangeGTIDS[length-1]
	if rangeGTID.End+1 == gno {
		rangeGTID.End = gno
		return
	}
	m[uuid] = append(rangeGTIDS, &RangeGTID{Start: gno, End: gno})
}

type RangeGTID struct {
	Start int64
	End   int64
}
