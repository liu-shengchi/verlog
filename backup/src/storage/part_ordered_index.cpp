#include "part_ordered_index.h"


/***************** OrderedNode *****************/

OrderedNode::OrderedNode(IndexKey index_key, Timestamp visible_ts, AccessObj* access_obj)
{
    index_key_  = index_key;
    visible_ts_ = visible_ts;
    access_obj_ = access_obj;

    diff_next_node_ = nullptr;
    same_next_node_ = nullptr;
}

OrderedNode::~OrderedNode()
{
}


int OrderedNode::compare(IndexKey index_key)
{
    if (index_key > index_key_)
        return 1;
    
    if (index_key < index_key_)
        return -1;

    return 0;
}

bool OrderedNode::IsVisible(Timestamp visible_ts)
{
    if (visible_ts >= visible_ts_)
        return true;
    
    return false;
}


/**************** PartitionIndex ***************/

OrderedIndex::OrderedIndex()
{
    first_node_ = nullptr;
}

OrderedIndex::~OrderedIndex()
{
}

RC OrderedIndex::ReadAccessObj(IndexKey key, AccessObj* &access_obj, Timestamp visible_ts)
{
    RC rc = RC_OK;

    db_rw_lock_.GetReadLock();

    OrderedNode* cur_node  = first_node_;
    OrderedNode* prev_node = nullptr;

    if (cur_node == nullptr)
    {
        printf("1\n");
    }

    while (cur_node != nullptr) {
        
        if (cur_node->IsVisible(visible_ts) && (cur_node->compare(key) >= 0))
            break;

        prev_node = cur_node;
        cur_node  = cur_node->diff_next_node_;
    }
    
    if (cur_node == nullptr)
    {
        access_obj = nullptr;
        rc = RC_NULL;
    }
    else
    {
        access_obj = cur_node->access_obj_;
        rc = RC_OK;
    }

    db_rw_lock_.ReleaseReadLock();

    return rc;
}


RC OrderedIndex::InsertAccessObj(IndexKey key, AccessObj* access_obj, Timestamp visible_ts, bool is_unique_index)
{
    RC rc = RC_OK;

    db_rw_lock_.GetWriteLock();

    OrderedNode* cur_node  = first_node_;
    OrderedNode* prev_node = nullptr;
    
    bool  exist_same_key = false;
    while (cur_node != nullptr) {
        if (cur_node->compare(key) == 1)
            break;
        else if (cur_node->compare(key) == -1) {
            prev_node = cur_node;
            cur_node = cur_node->diff_next_node_;
        }
        else {
            exist_same_key = true;
            break;
        }
    }

    if (!exist_same_key) {
        OrderedNode* new_node = new OrderedNode(key, visible_ts, access_obj);
        if (prev_node == nullptr) {           
            new_node->diff_next_node_ = first_node_;
            first_node_ = new_node;
        } else {
            new_node->diff_next_node_ = prev_node->diff_next_node_;
            prev_node->diff_next_node_ = new_node;
        }
    } else {
        if (is_unique_index) 
        {
            rc = RC_ERROR;
        }
        else
        {
            OrderedNode* new_node = new OrderedNode(key, visible_ts, access_obj);
            
            if (cur_node->IsVisible(visible_ts))
            {
                new_node->diff_next_node_ = cur_node->diff_next_node_;
                new_node->same_next_node_ = cur_node;
                
                cur_node->diff_next_node_ = nullptr;
                
                if (prev_node != nullptr)
                {
                    prev_node->diff_next_node_ = new_node;
                }
                else 
                {
                    first_node_ = new_node;
                }
                
            }
            else
            {
                prev_node = cur_node;
                cur_node  = cur_node->same_next_node_; 

                while (cur_node != nullptr)
                {
                    if (cur_node->IsVisible(visible_ts))
                    {
                        break;
                    }
                    else
                    {
                        prev_node = cur_node;
                        cur_node  = cur_node->same_next_node_; 
                    }
                }

                new_node->same_next_node_  = cur_node;
                prev_node->same_next_node_ = new_node;
            }
        }
    }

    db_rw_lock_.ReleaseWriteLock();

    return rc;
}

RC OrderedIndex::RemoveAccessObj(IndexKey key)
{
    RC rc = RC_OK;

    return rc;
}



/*************** PartOrderedindex **************/

PartOrderedindex::PartOrderedindex(uint64_t partition_cnt, TableID table_id, bool is_unique_index)
{
    partition_cnt_   = partition_cnt;
    ordered_indexes_ = new OrderedIndex[partition_cnt];

    table_id_        = table_id;
    is_unique_index_ = is_unique_index;
}

PartOrderedindex::~PartOrderedindex()
{
    delete[] ordered_indexes_;
}


uint64_t PartOrderedindex::PartHashFunc(PartitionKey part_key)
{
    return part_key % partition_cnt_;
}


RC  PartOrderedindex::IndexInsert(IndexKey index_key, AccessObj* access_obj, PartitionKey part_key, Timestamp visible_ts)
{
    RC rc = RC_OK;
    
    uint64_t      part_idx      = PartHashFunc(part_key);
    OrderedIndex* orderde_index = &ordered_indexes_[part_idx];

    rc = orderde_index->InsertAccessObj(index_key, access_obj, visible_ts, is_unique_index_);

    return rc;
}


RC  PartOrderedindex::IndexRead(IndexKey key, AccessObj* &access_obj, PartitionKey part_key, Timestamp visible_ts)
{
    RC rc = RC_OK;

    return rc;
}

//for non-primary key index, return the set of tuples that equal to indexkey
RC  PartOrderedindex::IndexRead(IndexKey key, AccessObj** &access_objs, uint64_t& ao_cnt, PartitionKey part_key, Timestamp visible_ts)
{
    RC rc = RC_OK;

    return rc;
}


//针对part_ordered_index设计
RC  PartOrderedindex::IndexMaxRead(IndexKey index_key, AccessObj* &access_obj, PartitionKey part_key, Timestamp visible_ts)
{
    RC rc = RC_OK;
    
    uint64_t      part_idx      = PartHashFunc(part_key);
    OrderedIndex* orderde_index = &ordered_indexes_[part_idx];

    rc = orderde_index->ReadAccessObj(index_key, access_obj, visible_ts);

    return rc;
}


RC  PartOrderedindex::IndexRemove(IndexKey key, PartitionKey part_key, Timestamp visible_ts)
{
    RC rc = RC_OK;

    return rc;
}
