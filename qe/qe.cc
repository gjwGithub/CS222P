
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
}

// ... the rest of your implementations go here
BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition,const unsigned numPages){
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
    this->numPages=numPages;
    this->leftIn->getAttributes(this->attrs_out);
	updateVectorOuter();
    this->rightIn->getAttributes(this->attrs_in);
    updateVectorInner();
}
RC BNLJoin::updateVectorOuter(){
    sum_buffer=0;
    outers.clear();
    while(sum_buffer<numPages*PAGE_SIZE){
        void *data=malloc(PAGE_SIZE);
        if(this->leftIn->getNextTuple(data) != QE_EOF){
            int sig_length;
            getByteLength(this->attrs_out,data,sig_length);
            sum_buffer += sig_length;
            Tuple tuple1(data,sig_length);
            outers.push_back(tuple1);
            free(data);
        }else{
            free(data);
            return -1;
        }
    }   
    return 0;
    
}
RC BNLJoin::updateVectorInner(){
    sum_buffer=0;
    inners.clear();
    while(sum_buffer<PAGE_SIZE){
        void *data=malloc(PAGE_SIZE);
        if(this->rightIn->getNextTuple(data) != QE_EOF){
            int sig_length;
            getByteLength(this->attrs_in,data,sig_length);
            sum_buffer += sig_length;
            Tuple tuple1(data,sig_length);
            inners.push_back(tuple1);
            free(data);
        }else{
            free(data);
            return -1;
        }
    }   
    return 0;
}

Tuple::Tuple(void *data,int length){
    this->length=length;
    this->data=malloc(length);
    memcpy((char *)this->data,(char *)data,length);
}

RC BNLJoin::getByteLength(vector<Attribute> attrs,void *data,int &size){
	int offset=0; // offset is the length of record
    int size1=attrs.size();
    int byte_count=ceil((double) size1 / 8);

    //calcuate the length of record;
    char* bites=(char*)malloc(byte_count);//the number of fields
    memcpy(bites,(char *)data + offset, byte_count);
    offset=offset+byte_count;
    for(int i=0;i<size1;i++){
        if(!(bites[i/8] & (1 << (7-i%8)))){
            if(attrs[i].type==TypeVarChar){
                int string_size;
                memcpy(&string_size,(char *)data + offset, 4);
                offset=offset+string_size+4;
            }else{
                offset=offset+4;
            }
        }
    }
    size=offset;
}
RC BNLJoin::find_r_value(int &attrtype,int &value_int,float &value_float,string &value_string,int attr_index){
    int offset=0; // offset is the length of record
    int size=attrs_out.size();
    int byte_count=ceil((double) size / 8);
    //calcuate the length of record;
    char* bites=(char*)malloc(byte_count);//the number of fields
    memcpy(bites,(char *)outers[outer_index].data + offset, byte_count);
    offset=offset+byte_count;
    for(int i=0;i<size;i++){
            if(!(bites[i/8] & (1 << (7-i%8)))){
                if(attrs_out[i].type==TypeVarChar){
                    int string_size;
                    memcpy(&string_size,(char *)outers[outer_index].data + offset, 4);
                    offset=offset+string_size+4;
                    if(i==attr_index){
                        attrtype=3;
                        char *value_char=(char *)malloc(string_size+1);
                        memcpy(value_char,(char *)outers[outer_index].data + offset-string_size,string_size);
                        value_char[string_size]='\0';
                        value_string=value_char;
                        break;
                    }
                }else if(attrs_out[i].type==TypeInt){
                    if(i==attr_index){
                        memcpy(&value_int,(char *)outers[outer_index].data + offset,sizeof(int));
                        attrtype=1;
                        break;
                    }
                    offset=offset+4;
                }else{
                    if(i==attr_index){
                        memcpy(&value_float,(char *)outers[outer_index].data + offset,sizeof(float));
                        attrtype=2;
                        break;
                    }
                    offset=offset+4;
                }
            }
        
    }
}
RC BNLJoin::find_s_value(int &attrtype,int &value_int,float &value_float,string &value_string,int inner_index,int attr_index){
    int offset=0; // offset is the length of record
    int size=attrs_in.size();
    int byte_count=ceil((double) size / 8);
    //calcuate the length of record;
    char* bites=(char*)malloc(byte_count);//the number of fields
    memcpy(bites,(char *)inners[inner_index].data + offset, byte_count);
    offset=offset+byte_count;
    for(int i=0;i<size;i++){
            if(!(bites[i/8] & (1 << (7-i%8)))){
                if(attrs_in[i].type==TypeVarChar){
                    int string_size;
                    memcpy(&string_size,(char *)inners[inner_index].data + offset, 4);
                    offset=offset+string_size+4;
                    if(i==attr_index){
                        attrtype=3;
                        char *value_char=(char *)malloc(string_size+1);
                        memcpy(value_char,(char *)outers[outer_index].data + offset-string_size,string_size);
                        value_char[string_size]='\0';
                        value_string=value_char;
                        break;
                    }
                }else if(attrs_in[i].type==TypeInt){
                    if(i==attr_index){
                        memcpy(&value_int,(char *)inners[inner_index].data + offset,sizeof(int));
                        attrtype=1;
                        break;
                    }
                    offset=offset+4;
                }else{
                    if(i==attr_index){
                        memcpy(&value_float,(char *)inners[inner_index].data + offset,sizeof(float));
                        attrtype=2;
                        break;
                    }
                    offset=offset+4;
                }
            }
        
    }
}
RC BNLJoin::getNextTuple(void *data){
    while(true){
    while(outer_index<outers.size()){
    int attr_index;
    for(int i=0;i<attrs_out.size();i++){
        if(attrs_out[i].name==condition.lhsAttr){
            attr_index=i;
            break;
        }
    }
    //find the R value
    int attrtype=0;
    int value_int;
    float value_float;
    string value_string;
    find_r_value(attrtype,value_int,value_float,value_string,attr_index);
    //find s value
    int attr_right_index;
    for(int i=0;i<attrs_in.size();i++){
        if(attrs_in[i].name==condition.rhsAttr){
            attr_right_index=i;
            break;
        }
    }
    for(int i=inner_index;i<inners.size();i++){
        int attrtype1=0;
        int value_int1;
        float value_float1;
        string value_string1;
        find_s_value(attrtype1,value_int1,value_float1,value_string1,i,attr_right_index);
        if(attrtype==1){
            if(value_int1==value_int){
                getJoin(data,outers[outer_index].data,outers[outer_index].length,inners[i].data,inners[i].length);
                inner_index=i+1;
                return 0;
            }
        }else if(attrtype==2){
            if(value_float==value_float1){
                getJoin(data,outers[outer_index].data,outers[outer_index].length,inners[i].data,inners[i].length);
                inner_index=i+1;
                return 0;
            }
        }else if(attrtype==3){
            if(value_string1==value_string){
                getJoin(data,outers[outer_index].data,outers[outer_index].length,inners[i].data,inners[i].length);
                inner_index=i+1;
                return 0;
            }
        }
    }
    outer_index++;
    }
    if(outer_index>=outers.size()){
        RC rel=updateVectorInner();
        inner_index=0;
        outer_index=0;
        if(inners.size()==0){
            RC rel1=updateVectorOuter();
            inner_index=0;
            outer_index=0;
            rightIn->setIterator();
            RC rel2=updateVectorInner();
            if(outers.size()==0)
                return QE_EOF;
        }
    }
}
}
RC BNLJoin::getJoin(void *data,void *r_data,int outer_length,void *s_data,int inner_length){
    int offset=0; // offset is the length of record
    int size=attrs_out.size();
    int byte_count=ceil((double) size / 8);
    char* bites=(char*)malloc(byte_count);//the number of fields
    memcpy(bites,(char *)outers[outer_index].data + offset, byte_count);

    int offset1=0;
    int size1=attrs_in.size();
    int byte_count1=ceil((double) size1 / 8);
    char* bites1=(char*)malloc(byte_count1);//the number of fields
    memcpy(bites1,(char *)outers[outer_index].data + offset1, byte_count1);


    int join_size = size+size1;
    int join_byte_count=ceil((double)join_size / 8);
    //calcuate the length of record;
    int join_offset=join_byte_count;
   
    memcpy((char *)data,bites,byte_count);
    for(int i=size;i<join_size;i++){
        if(!(bites1[(i-size)/8]&(1<<(7-(i-size)%8))));
        else{
            ((char *)data)[i/8]=((char *)data)[i/8]|(1<<(7-(i-size)%8));
        }
    }
    memcpy((char *)data+join_offset,(char *)r_data+byte_count,outer_length-byte_count);
    memcpy((char *)data+join_offset+outer_length-byte_count,(char *)s_data+byte_count1,inner_length-byte_count1);
}
void BNLJoin::getAttributes(vector<Attribute> &attrs) const{
    for(int i=0;i<attrs_out.size();i++){
        attrs.push_back(attrs_out[i]);
    }
    for(int i=0;i<attrs_in.size();i++){
        attrs.push_back(attrs_in[i]);
    }
}
