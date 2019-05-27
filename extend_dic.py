# -*- coding: utf-8 -*-
'''
Created on 26.09.2016

@author: jwulf
'''
##for reasons of simplicity add and afterwords remove % at eof
####################### please define input parameters here


#define new construct as list of words (characters must all be lower case!!)
#construct_list1 = ['letter','mail','e-message','chat*','correspondence','voic*','call','phone','ring','contact','visit','offline','e-mail']
construct_list1 = ['letter','mail','e-message','chat*','correspondence','voic*','call','phone','ring','contact','visit','offline','e-mail','send']
concept_name1 = 'new_resp_mod'

construct_list2 = ['apolog*','sorry','confess*','admit','pardon','guilt','guilty','excus*','acknowledg*','assert','conced*','recogni*','accept','agree','approv*','forgiv*','accident*']
concept_name2 = 'new_apology'

#construct_list3 =  ['offer*','compensat*','refund*','reimburs*','repay*','restor*','return*','recover*']
construct_list3 =  ['compensat*','refund*','reimburs*','repay*','restor*','return*','recover*','replace*','alternativ*']

concept_name3 = 'new_modification'

construct_list4 =  ['aberration','abhor*','abject','abnormal','abominat*','appall*','averse','bloated','constipat*','contaminate','contaminated','crap','cringeworthy','decay*','degrad*','deteriorated','detestation','dirt','dirty','disgust*','distast*','distorted','dung','entrails','excrement','execration','feces','filth','filthy','flabby','flatulence','gaby','gag','garbage','grime','grimy','gross','grotesque','gruesome','gutter','heretic','herpes*','hideous','incest*','infestation','latrines','lewd','loathing','loo','maggot','mess','messy','mildew','mire','muck','muddy','musty','mutilated','nause*','nauseous','obscene','ooze','perver*','pig*','pollut*','puk*','pungen*','purgator*','rags','rancid','regurgitation','repel*','repelling','repugnan*','repuls*','revuls*','rot','rotting','rubbish','scum','sewage','sewer','sewerage','shabby','sicken*','sickness','slime','slimy','slop','sloth','sludge','spew','spit','squirm','stain','sticky','stink','stinking','swig','tasteless','toad','trash','trashy','ugly','unclean','untidy','unwashed','vomit','vulgar','wart','weird','withered']
concept_name4 = 'new_disgust'

#define new constructs in a dictionary
construct_collection = {concept_name1:construct_list1,concept_name2:construct_list2,concept_name3:construct_list3,concept_name4:construct_list4}

#define dictionary in code folder to be modified
dic_name = 'LIWC2015_English_JWU.dic'

##############################################
dic_collector=list()
out_collector=list()
#read dict to list
construct_dic_file = open(dic_name,"r")
for readl in construct_dic_file:
    dic_collector.append(readl)
dic_length=len(dic_collector)

for concept_name, construct_list in construct_collection.iteritems():

    #sort the list alphabetically
    construct_list_sorted = sorted(construct_list)

    #read in dict (must be sorted...)    
    dic_line_pre = dic_collector[0]
    dic_line_post = dic_collector[1]
    out_collector=list(dic_line_pre.rstrip())
    
    dic_index=2
    
    #read dic line by line until end of concept part and add new concept
    #thereby remember the number of the last concept (again, must be sorted!!!)
    #allocate new number to new concept
    while dic_line_post.find("%") != 0:        
        out_collector.append(dic_line_post.rstrip())        
        dic_line_pre = dic_line_post
        dic_line_post = dic_collector[dic_index]
        dic_index=dic_index+1
    concept_num = int(dic_line_pre.split('\t')[0])+1
    
    #TODO
    out_collector.append(str(concept_num)+'\t'+concept_name)
    out_collector.append(dic_line_post.rstrip())
    
    
    
    construct_list_iterator=0    
    construct_word = construct_list_sorted[construct_list_iterator]
    construct_word_list = list(construct_word)
    constructs_empty = 0
    construct_word_position = 0
    
    #iterate over the dic_word list
    for dic_counter in range (dic_index,dic_length):
        dic_line=dic_collector[dic_counter]
        if constructs_empty==1:
            #print the rest of the dic
            out_collector.append(dic_line.rstrip())
            for dic_counter2 in range(dic_counter,dic_length):
                dic_line=dic_collector[dic_counter2]
                out_collector.append(dic_line.rstrip())
            break

        dic_word = dic_line.split('\t')[0]
        dic_word_list = list(dic_word)
        next_line = 0
        
        #iterate over construct_word_position
        while next_line == 0:
                    
            if dic_word_list[construct_word_position]=="*" and construct_word_list[construct_word_position]!="*":
                print("error with *s. contruct word: "+construct_word+"  dic word: "+dic_word)            
                if (construct_list_iterator+1)<len(construct_list1):
                    #get new construct word
                    construct_list_iterator=construct_list_iterator+1
                    construct_word = construct_list_sorted[construct_list_iterator]
                    construct_word_list = list(construct_word)
                    construct_word_position = 0
                    next_line = 1
                else:
                    #no more construct words
                    constructs_empty=1
                    next_line = 1
            elif dic_word_list[construct_word_position]!="*" and construct_word_list[construct_word_position]=="*":
                print("error with *s. contruct word: "+construct_word+"  dic word: "+dic_word)
                if (construct_list_iterator+1)<len(construct_list):
                    #get new construct word
                    construct_list_iterator=construct_list_iterator+1
                    construct_word = construct_list_sorted[construct_list_iterator]
                    construct_word_list = list(construct_word)
                    construct_word_position = 0
                    next_line = 1
                else:
                    #no more construct words
                    constructs_empty=1
                    next_line = 1
            #if dic word vor construct_word
            elif dic_word_list[construct_word_position] < construct_word_list[construct_word_position]:
                construct_word_position = 0
                next_line = 1
                out_collector.append(dic_line.rstrip())
            #if dic word NACH construct_word
            elif dic_word_list[construct_word_position] > construct_word_list[construct_word_position]:
                #TODO write construct word
                out_collector.append(construct_word+'\t'+str(concept_num))
                #still words in constructlist left?
                if (construct_list_iterator+1)<len(construct_list):
                    #get new construct word
                    construct_list_iterator=construct_list_iterator+1
                    construct_word = construct_list_sorted[construct_list_iterator]
                    construct_word_list = list(construct_word)
                    construct_word_position = 0
                else:
                    #no more construct words
                    constructs_empty=1
                    next_line = 1
            #character match
            elif dic_word_list[construct_word_position] == construct_word_list[construct_word_position]:
                #do we have a match?
                if (construct_word_position+1)==len(dic_word_list) and (construct_word_position+1)==len(construct_word_list):
                    #TODO add number to given word
                    out_collector.append(dic_line.rstrip()+'\t'+str(concept_num))
                    #still words in constructlist left?
                    if (construct_list_iterator+1)<len(construct_list):
                        #get new construct word
                        construct_list_iterator=construct_list_iterator+1
                        construct_word = construct_list_sorted[construct_list_iterator]
                        construct_word_list = list(construct_word)
                        construct_word_position = 0
                        next_line = 1
                    else:
                        #no more construct words
                        
                        constructs_empty=1
                        next_line = 1
                        
                #construct word AFTER dic word
                elif (construct_word_position+1)>=len(dic_word_list) and (construct_word_position+1)<len(construct_word_list):
                    construct_word_position = 0
                    next_line = 1
                    out_collector.append(dic_line.rstrip())
                #construct word BEFORE dic word
                elif (construct_word_position+1)<len(dic_word_list) and (construct_word_position+1)>=len(construct_word_list):
                    #TODO add number to given word
                    out_collector.append(construct_word+'\t'+str(concept_num))
                    #still words in constructlist left?
                    if (construct_list_iterator+1)<len(construct_list):
                        #get new construct word
                        construct_list_iterator=construct_list_iterator+1
                        construct_word = construct_list_sorted[construct_list_iterator]
                        construct_word_list = list(construct_word)
                        construct_word_position = 0
    
                    else:
                        #no more construct words
                        constructs_empty=1
                        next_line = 1
                elif (construct_word_position+1)>=len(dic_word_list) and (construct_word_position+1)>=len(construct_word_list):
                    print("Error - unequal word length. contruct word: "+construct_word+"  dic word: "+dic_word)
                    if (construct_list_iterator+1)<len(construct_list):
                        #get new construct word
                        construct_list_iterator=construct_list_iterator+1
                        construct_word = construct_list_sorted[construct_list_iterator]
                        construct_word_list = list(construct_word)
                        construct_word_position = 0
                    else:
                        #no more construct words
                        constructs_empty=1
                        next_line = 1
                else:
                    #try next position
                    construct_word_position=construct_word_position+1
    #prepare for next iteration
    dic_collector=list()
    dic_collector=out_collector 
    dic_length=len(dic_collector) 
    out_collector=list() 
    
#print(out_collector)
out_file=open('newEngJwu.dic','w')
for item in dic_collector:
    out_file.write("%s\n" % item)