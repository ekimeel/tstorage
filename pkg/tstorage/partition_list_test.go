package tstorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_partitionList_Remove(t *testing.T) {
	tests := []struct {
		name              string
		partitionList     partitionListImpl
		target            partition
		wantErr           bool
		wantPartitionList partitionListImpl
	}{
		{
			name:          "empty partition",
			partitionList: partitionListImpl{},
			wantErr:       true,
		},
		{
			name: "remove the head node",
			partitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &mockPartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &mockPartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			target: &mockPartition{
				minT: 1,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 1,
				head: &partitionNode{
					val: &mockPartition{
						minT: 2,
					},
				},
				tail: &partitionNode{
					val: &mockPartition{
						minT: 2,
					},
				},
			},
		},
		{
			name: "remove the tail node",
			partitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &mockPartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &mockPartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			target: &mockPartition{
				minT: 2,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 1,
				head: &partitionNode{
					val: &mockPartition{
						minT: 1,
					},
				},
				tail: &partitionNode{
					val: &mockPartition{
						minT: 1,
					},
				},
			},
		},
		{
			name: "remove the middle node",
			partitionList: func() partitionListImpl {
				third := &partitionNode{
					val: &mockPartition{
						minT: 3,
					},
				}
				second := &partitionNode{
					val: &mockPartition{
						minT: 2,
					},
					next: third,
				}
				first := &partitionNode{
					val: &mockPartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 3,
					head:          first,
					tail:          third,
				}
			}(),
			target: &mockPartition{
				minT: 2,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 2,
				head: &partitionNode{
					val: &mockPartition{
						minT: 1,
					},
					next: &partitionNode{
						val: &mockPartition{
							minT: 3,
						},
					},
				},
				tail: &partitionNode{
					val: &mockPartition{
						minT: 3,
					},
				},
			},
		},
		{
			name: "given node not found",
			partitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &mockPartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &mockPartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			target: &mockPartition{
				minT: 3,
			},
			wantPartitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &mockPartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &mockPartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.partitionList.remove(tt.target)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.wantPartitionList, tt.partitionList)
		})
	}
}

func Test_partitionList_Swap(t *testing.T) {
	tests := []struct {
		name              string
		partitionList     partitionListImpl
		old               partition
		new               partition
		wantErr           bool
		wantPartitionList partitionListImpl
	}{
		{
			name:          "empty partition",
			partitionList: partitionListImpl{},
			wantErr:       true,
		},
		{
			name: "swap the head node",
			partitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &mockPartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &mockPartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			old: &mockPartition{
				minT: 1,
			},
			new: &mockPartition{
				minT: 100,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 2,
				head: &partitionNode{
					val: &mockPartition{
						minT: 100,
					},
					next: &partitionNode{
						val: &mockPartition{
							minT: 2,
						},
					},
				},
				tail: &partitionNode{
					val: &mockPartition{
						minT: 2,
					},
				},
			},
		},
		{
			name: "swap the tail node",
			partitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &mockPartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &mockPartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			old: &mockPartition{
				minT: 2,
			},
			new: &mockPartition{
				minT: 100,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 2,
				head: &partitionNode{
					val: &mockPartition{
						minT: 1,
					},
					next: &partitionNode{
						val: &mockPartition{
							minT: 100,
						},
					},
				},
				tail: &partitionNode{
					val: &mockPartition{
						minT: 100,
					},
				},
			},
		},
		{
			name: "swap the middle node",
			partitionList: func() partitionListImpl {
				third := &partitionNode{
					val: &mockPartition{
						minT: 3,
					},
				}
				second := &partitionNode{
					val: &mockPartition{
						minT: 2,
					},
					next: third,
				}

				first := &partitionNode{
					val: &mockPartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 3,
					head:          first,
					tail:          third,
				}
			}(),
			old: &mockPartition{
				minT: 2,
			},
			new: &mockPartition{
				minT: 100,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 3,
				head: &partitionNode{
					val: &mockPartition{
						minT: 1,
					},
					next: &partitionNode{
						val: &mockPartition{
							minT: 100,
						},
						next: &partitionNode{
							val: &mockPartition{
								minT: 3,
							},
						},
					},
				},
				tail: &partitionNode{
					val: &mockPartition{
						minT: 3,
					},
				},
			},
		},
		{
			name: "given node not found",
			partitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &mockPartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &mockPartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			old: &mockPartition{
				minT: 100,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 2,
				head: &partitionNode{
					val: &mockPartition{
						minT: 1,
					},
					next: &partitionNode{
						val: &mockPartition{
							minT: 2,
						},
					},
				},
				tail: &partitionNode{
					val: &mockPartition{
						minT: 2,
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.partitionList.swap(tt.old, tt.new)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.wantPartitionList, tt.partitionList)
		})
	}
}
