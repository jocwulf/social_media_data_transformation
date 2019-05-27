# -*- coding: utf-8 -*-
###TODO configure language config in setlanguage in else for english and german


###do not forget to add the new keys if needed
###126    new_apology
###127    new_resp_mod
###128    new_modification

####CHANGES####
#20161214: In read token change sequence: pull $ before *
#20161214: Inlcuded langprob info, set to 0.8
#....
#20180308: Include disgust dictionary and sentence logic
#20180309: add two grams
from nltk.tokenize import sent_tokenize, word_tokenize
import re
from base import Lexicon
from collections import Counter
import json
import pandas as pd
import ahocorasick
import os
import csv



class Liwc(Lexicon):
            
    
    corpus_filepath_en = './newEngJwu.trie'
    corpus_filepath_ger = './newGerJwu.trie'

    # category analysis variables:
#     category_keys = ['funct', 'pronoun', 'ppron', 'i', 'we', 'you', 'shehe',
#         'they', 'ipron', 'article', 'verb', 'auxverb', 'past', 'present', 'future',
#         'adverb', 'preps', 'conj', 'negate', 'quant', 'number', 'swear', 'social',
#         'family', 'friend', 'humans', 'affect', 'posemo', 'negemo', 'anx', 'anger',
#         'sad', 'cogmech', 'insight', 'cause', 'discrep', 'tentat', 'certain',
#         'inhib', 'incl', 'excl', 'percept', 'see', 'hear', 'feel', 'bio', 'body',
#         'health', 'sexual', 'ingest', 'relativ', 'motion', 'space', 'time', 'work',
#         'achieve', 'leisure', 'home', 'money', 'relig', 'death', 'assent', 'nonfl',
#         'filler']
    
    category_keys = ['function','pronoun','ppron','i','we','you','shehe','they','ipron','article','prep','auxverb','adverb','conj','negate','verb','adj','compare','interrog','number','quant','affect','posemo','negemo','anx','anger','sad','social','family','friend','female','male','cogproc','insight','cause','discrep','tentat','certain','differ','percept','see','hear','feel','bio','body','health','sexual','ingest','drives','affiliation','achiev','power','reward','risk','focuspast','focuspresent','focusfuture','relativ','motion','space','time','work','leisure','home','money','relig','death','informal','swear','netspeak','assent','nonflu','filler','new_disgust', 'new_apology','new_resp_mod','new_modification','posemo_bi', 'negemo_bi', 'anger_bi', 'anx_bi', 'sad_bi', 'new_disgust_bi', 'negemo_bi_rev', 'posemo_bi_rev', 'negemo_sen', 'posemo_sen', 'anger_sen', 'anx_sen', 'sad_sen', 'new_disgust_sen', 'posemo_sen_rev','negemo_sen_rev','negate_bi']
    
    
    # full analysis variables:
    meta_keys = ['WC', 'WPS', 'Sixltr', 'Dic', 'Numerals']
    puncuation_keys = [
        'Period', 'Comma', 'Colon', 'SemiC', 'QMark', 'Exclam',
        'Dash', 'Quote', 'Apostro', 'Parenth', 'OtherP', 'AllPct']
    punctuation = [
        ('Period', '.'),
        ('Comma', ','),
        ('Colon', ':'),
        ('SemiC', ';'),
        ('QMark', '?'),
        ('Exclam', '!'),
        ('Dash', '-'),  # –—
        ('Quote', '"'),  # “”
        ('Apostro', "'"),  # ‘’
        ('Parenth', '()[]{}'),
        ('OtherP', '#$%&*+-/<=>@\\^_`|~')
    ]

    def __init__(self):
        
        self.language = "en"

        #jwu further boolean characteristics
        self.mail = False
        self.link = False
        
        #jwu changed + self.category_keys[:-1]
        self.all_keys = self.meta_keys + self.category_keys + self.puncuation_keys


        with open(self.corpus_filepath_en) as corpus_file1:
            self._trie_en = json.load(corpus_file1)

        with open(self.corpus_filepath_ger) as corpus_file2:
            self._trie_de = json.load(corpus_file2)
        
        #set default to English
        self._trie = self._trie_en      
        
        ##read emoji and emoticon tables and prepare trie
        file_path = os.path.dirname(os.path.abspath(__file__))
        #emoji_key = pd.read_csv(file_path + '\emoji_table.txt', encoding='utf-8')
        emoji_key = pd.read_csv(file_path + '/emoji_table.txt', encoding='utf-8', index_col=0)
        emoji_key['count'] = 0
        emoji_dict = emoji_key['count'].to_dict()
        
        self.emoji_trie = ahocorasick.Automaton()
        for emkey in emoji_dict.keys():
            emkeys=emkey[0].encode('utf8')
            self.emoji_trie.add_word(emkeys,emkeys)
        
        g = csv.reader(open(file_path + '/emoticon_table.txt',"rb"), delimiter='\t')
        for emkey in g:   
            emkeys=emkey[0]
            self.emoji_trie.add_word(emkeys,emkeys)
        
        self.emoji_trie.make_automaton()
        self.emocount = 0
                
    # standard Lexicon functionality:
    
    def set_language(self, lang, langprob):
        if not (self.language == lang):
            if lang == "en" and langprob >= 0.8:
                self._trie = self._trie_en
                self.language="en"
            elif lang == "de" and langprob >= 0.8:
                self._trie = self._trie_de
                self.language="de"
            else:  #ATTENTION this is where primary language is set
                self._trie = self._trie_en
                self.language="en"
                
    
    def set_link(self, booli):
        if booli:
            self.link = True
        else:
            self.link = False
            
    def reset_mail(self):
        self.mail = False
    
    def return_mail(self):
        return self.mail
    
    def read_token(self, token, token_i=0, trie_cursor=None):
        #jwu added tokencounter at some places
        global tokencounter
        if trie_cursor is None:
            trie_cursor = self._trie

        #jwu * and $ switched
        if '$' in trie_cursor and token_i == len(token):
            tokencounter = tokencounter + 1
            for category in trie_cursor['$']:
                yield category.encode(encoding="utf-8")   
        elif '*' in trie_cursor:
            tokencounter = tokencounter + 1
            for category in trie_cursor['*']:
                yield category.encode(encoding="utf-8")
            if token_i < len(token):
                letter = token[token_i]
                if letter in trie_cursor:
                    for category in self.read_token(token, token_i + 1, trie_cursor[letter]):
                        yield category
        elif token_i < len(token):
            letter = token[token_i]
            if letter in trie_cursor:
                for category in self.read_token(token, token_i + 1, trie_cursor[letter]):
                    yield category

    #jwu - in regular expression added, also optional langinfo
    def read_document(self, document, token_pattern=ur"[a-zöäüÖÄÜß]['-'a-zöäüß]*"):
        global tokencounter
        #tokencounter=0
        
        #implement the 2gram approach with negation
        prenegate=0
        preposemo=0
        prenegemo=0
        preanger=0
        preanx=0
        presad=0
        prenew_disgust=0
        
        
        #for match in re.finditer(token_pattern, document.lower(),re.UNICODE):
        for match in word_tokenize(document.lower()):
                            
            prenegate_flag=0                              
            preposemo_flag=0                   
            prenegemo_flag=0                 
            preanger_flag=0                  
            preanx_flag=0           
            presad_flag=0            
            prenew_disgust_flag=0
            
            for category in self.read_token(match):
                yield category
                if category=="posemo":
                    preposemo_flag=1                  
                    if prenegate!=0:
                        yield "posemo_bi"
                elif category=="negemo":                  
                    prenegemo_flag=1
                    if prenegate!=0:
                        yield "negemo_bi"
                elif category=="anger":                  
                    preanger_flag=1
                    if prenegate!=0:
                        yield "anger_bi"
                elif category=="anx":                  
                    preanx_flag=1
                    if prenegate!=0:
                        yield "anx_bi"
                elif category=="sad":                  
                    presad_flag=1
                    if prenegate!=0:
                        yield "sad_bi"
                elif category=="new_disgust":                  
                    prenew_disgust_flag=1
                    if prenegate!=0:
                        yield "new_disgust_bi" 
                elif category=="negate":                  
                    prenegate_flag=1
                    if preposemo!=0:                           
                        yield "posemo_bi"
                    if prenegemo!=0:                            
                        yield "negemo_bi"
                    if preanger!=0:          
                        yield "anger_bi"
                    if preanx!=0:          
                        yield "anx_bi"
                    if presad!=0:          
                        yield "sad_bi"
                    if prenew_disgust!=0:          
                        yield "new_disgust_bi"
                                    
            if prenegate_flag!=0:                  
                prenegate=1
            else:
                prenegate=0                
            if preposemo_flag!=0:                  
                preposemo=1 
            else:
                preposemo=0
            if prenegemo_flag!=0:                  
                prenegemo=1
            else:
                prenegemo=0
            if preanger_flag!=0:                  
                preanger=1
            else:
                preanger=0
            if preanx_flag!=0:                  
                preanx=1
            else:
                preanx=0
            if presad_flag!=0:              
                presad=1
            else:
                presad=0
            if prenew_disgust_flag!=0:              
                prenew_disgust=1
            else:
                prenew_disgust=0
    #jwu newly added
    def read_document_specials(self, document):
        if self.language=="en":
            if "like" in document:
                for match in re.finditer(r"([a-z][-'a-z]*\s)?([a-z][-'a-z]*\s)?like[-'a-z]*", document.lower()):
                    likestring=match.group(0)
                    #Is there a word before like
                    splittedstring=likestring.split()
                    stringlen=len(splittedstring)
                    likewordlength=len(splittedstring[stringlen-1])
                    if stringlen == 1:
                        if likewordlength==4:
                            
                            yield "function"    
                            yield "prep"
                            yield "compare"
        
                    else:
        
                        #lets take the last two and match
                        if splittedstring[stringlen-2] in ["i","you","we","they", "to"]:
                            
                            yield "affect"
                            yield "posemo" 
                            yield "verb"
                            yield "focuspresent"
                            
#                         nur den Positivcase nehmen...    
#                         elif likewordlength==4 and splittedstring[stringlen-2] in ["did","didn't","do","does","doesn't","don't","will","won't"]:
#                             
#                             yield "affect"
#                             yield "posemo"  
#                         elif (likewordlength==4 ) and (splittedstring[stringlen-2]=="not") and (splittedstring[stringlen-3] in ["could","did","do","does","should","will","would"]):
#                             
#                             yield "affect"
#                             yield "posemo"                                                                                                   

                        elif likewordlength==4 and splittedstring[stringlen-2] in ["did","do","does","will"]:
                            
                            yield "affect"
                            yield "posemo" 
            #now kind of
            for match in re.finditer(r"kind\sof", document.lower()):
                yield "cogproc"
                yield "tentat"
            
        #JWU Email identification not just important for english
        if re.search("([^@|\s]+@[^@]+\.[^@|\s]+)",document.lower(),re.I):
            self.mail = True
            yield "new_resp_mod"
            
        #JWU Used this here to extract weblinks: http://daringfireball.net/2009/11/liberal%5Fregex%5Ffor%5Fmatching%5Furls
        #if self.link or re.search("\b(([\w-]+://?|www[.])[^\s()<>]+(?:\([\w\d]+\)|([^[:punct:]\s]|/)))",document.lower(),re.I):
        if self.link or re.search("www|http",document.lower(),re.I):
                    yield "new_resp_mod"
        

    # extra (legacy) Liwc functionality:

    #jwu count emojis and emoticons
    def count_emos(self, document):
        docencoded = document.encode('utf-8')
       
        matchcontainer = self.emoji_trie.iter(docencoded)
        for obj in matchcontainer:
            
            self.emocount += 1
            yield 'netspeak'
    
    #jwu - in regular expression added, also language info
    def summarize_document(self, document, token_pattern=ur"[a-zöäüÖÄÜß]['-'a-zöäüß]*", normalize=True, langinfo="en", langprob=1):
        global tokencounter
        tokencounter = 0
        self.set_language(langinfo, langprob)
        #jwu - sentence end must be preceded by a letter
        #sentence_count = len(re.findall(ur"[a-zöäüß][.!?]+", document)) or 1

        # tokens is a bit redundant because it duplicates the tokenizing done
        # in read_document, but to keep read_document simple, we just run it again here.
        
        #split into sentences, than iterate over sentences
        
        tokens = re.findall(token_pattern, document.lower())
        sentence_list = sent_tokenize(document)
        sentence_count = len(sentence_list) if len(sentence_list) > 0 else 1
        counts1 = Counter()
        for sentence in sentence_list:
            counts_sentence = Counter(self.read_document(sentence, token_pattern=token_pattern))
            counts_sentence['negate_bi'] = 0
            #this is the bigram-based approach
            ## this is reversing negemo and posemo
            counts_sentence['negemo_bi_rev']=counts_sentence['negemo']
            counts_sentence['posemo_bi_rev']=counts_sentence['posemo']
            if 'negemo_bi' in counts_sentence and counts_sentence['negemo_bi']>0:
                counts_sentence['negate_bi'] = 1
                if 'posemo' not in counts_sentence or counts_sentence['posemo']==0:
                    counts_sentence['negemo_bi_rev'] = counts_sentence['negemo_bi_rev'] - counts_sentence['negemo_bi'] #keep only the negemo that is not reversed
                    counts_sentence['posemo_bi_rev'] = counts_sentence['negemo_bi']
                else:
                    counts_sentence['negemo_bi_rev'] = 0
                    counts_sentence['posemo_bi_rev'] = 0
                    
                                        
            if 'posemo_bi' in counts_sentence and counts_sentence['posemo_bi']>0:
                counts_sentence['negate_bi'] = 1
                if 'negemo' not in counts_sentence or counts_sentence['negemo']==0:
                    counts_sentence['negemo_bi_rev'] = counts_sentence['posemo_bi']
                    counts_sentence['posemo_bi_rev'] = counts_sentence['posemo_bi_rev'] - counts_sentence['posemo_bi'] #keep only non reversed
                else:
                    counts_sentence['negemo_bi_rev'] = 0
                    counts_sentence['posemo_bi_rev'] = 0                    
            
            ## this is setting to zero
            if 'negemo_bi' not in counts_sentence:
                counts_sentence['negemo_bi']=counts_sentence['negemo']
            elif counts_sentence['negemo_bi']==0:
                counts_sentence['negemo_bi']=counts_sentence['negemo']
            else:
                counts_sentence['negemo_bi']=0
                
            if 'posemo_bi' not in counts_sentence:
                counts_sentence['posemo_bi']=counts_sentence['posemo']
            elif counts_sentence['posemo_bi']==0:
                counts_sentence['posemo_bi']=counts_sentence['posemo']
            else:
                counts_sentence['posemo_bi']=0
                
            if 'anger_bi' not in counts_sentence:
                counts_sentence['anger_bi']=counts_sentence['anger']
            elif counts_sentence['anger_bi']==0:
                counts_sentence['anger_bi']=counts_sentence['anger']
            else:
                counts_sentence['anger_bi']=0    
                
            if 'anx_bi' not in counts_sentence:
                counts_sentence['anx_bi']=counts_sentence['anx']
            elif counts_sentence['anx_bi']==0:
                counts_sentence['anx_bi']=counts_sentence['anx']
            else:
                counts_sentence['anx_bi']=0   
                
            if 'sad_bi' not in counts_sentence:
                counts_sentence['sad_bi']=counts_sentence['sad']
            elif counts_sentence['sad_bi']==0:
                counts_sentence['sad_bi']=counts_sentence['sad']
            else:
                counts_sentence['sad_bi']=0
                
            if 'new_disgust_bi' not in counts_sentence:
                counts_sentence['new_disgust_bi']=counts_sentence['new_disgust']
            elif counts_sentence['new_disgust_bi']==0:
                counts_sentence['new_disgust_bi']=counts_sentence['new_disgust']
            else:
                counts_sentence['new_disgust_bi']=0        
                
            
            #this is the sentence based approach
            counts_sentence['negemo_sen_rev']=counts_sentence['negemo']
            counts_sentence['posemo_sen_rev']=counts_sentence['posemo']                         
            counts_sentence['negemo_sen'] = counts_sentence['negemo']
            counts_sentence['posemo_sen'] = counts_sentence['posemo']
            counts_sentence['anger_sen'] = counts_sentence['anger']
            counts_sentence['anx_sen'] = counts_sentence['anx']
            counts_sentence['sad_sen'] = counts_sentence['sad']
            counts_sentence['new_disgust_sen'] = counts_sentence['new_disgust']
            
            if counts_sentence['negate']>0:
                if counts_sentence['negemo']>0 and counts_sentence['posemo']>0:
                    counts_sentence['negemo_sen_rev']=0
                    counts_sentence['posemo_sen_rev']=0
                elif counts_sentence['negemo']==0 and counts_sentence['posemo']>0:
                    counts_sentence['negemo_sen_rev']=counts_sentence['posemo']
                    counts_sentence['posemo_sen_rev']=0
                elif counts_sentence['negemo']>0 and counts_sentence['posemo']==0:
                    counts_sentence['posemo_sen_rev']=counts_sentence['negemo']
                    counts_sentence['negemo_sen_rev']=0
                
                counts_sentence['negemo_sen'] = 0
                counts_sentence['posemo_sen'] = 0
                counts_sentence['anger_sen'] = 0
                counts_sentence['anx_sen'] = 0
                counts_sentence['sad_sen'] = 0
                counts_sentence['new_disgust_sen'] = 0
                
            counts1 = {key: counts1.get(key, 0) + counts_sentence.get(key, 0) for key in set(counts1) | set(counts_sentence)}
            
        self.emocount= 0
        countsemos = Counter(self.count_emos(document))
     
        
        
        counts2 = Counter(self.read_document_specials(document))
        counts = Counter(counts1) + counts2 + countsemos

        counts['WC'] = len(tokens) + self.emocount
        
        #jwu changed here
        #counts['Dic'] = sum(counts.values())
        counts['Dic'] = tokencounter
        counts['WPS'] = counts['WC'] / float(sentence_count)
        counts['Sixltr'] = sum(len(token) > 6 for token in tokens)
        counts['Numerals'] = sum(token.isdigit() for token in tokens)

        # count up all characters so that we can get punctuation counts quickly
        character_counts = Counter(document)
        for name, chars in self.punctuation:
            counts[name] = sum(character_counts[char] for char in chars)
        # Parenth is special -- we only count one half of them (to match the official LIWC application)
        counts['Parenth'] = counts['Parenth'] / 2.0
        counts['AllPct'] = sum(counts[name] for name, _ in self.punctuation)
        
        #now we have to translate german into english dic keys
        if self.language == "de":
            counts['cogproc'] = counts['cogmech']
            counts['filler'] = counts['fillers']
            counts['prep'] = counts['preps']

        if normalize:
            # normalize all counts but the first two ('WC' and 'WPS')
            for column in self.all_keys[2:]:
                #jwu ergänzt
                if float(counts['WC'])>0:
                    counts[column] = float(counts[column]) / float(counts['WC'])
                else:
                    counts[column] = 0
        
        # return a normal dict() rather than the Counter() instance
        result = dict.fromkeys(self.category_keys + ['Dic'], 0)
        result.update(counts)
        
        return result

    def print_summarization(self, counts):
        absolutes = ['%d' % counts['WC'], '%0.2f' % counts['WPS']]
        percentages = ['%0.2f' % (counts[key] * 100) for key in self.all_keys[2:]]

        for key, value in zip(self.all_keys, absolutes + percentages):
            print '%16s %s' % (key, value)
            
    #jwu added
    def print_tabbed_summarization(self, counts):
        absolutes = ['%d' % counts['WC'], '%0.2f' % counts['WPS']]
        percentages = ['%0.2f' % (counts[key] * 100) for key in self.all_keys[2:]]
        liwc_resultlist = []
        
        for key, value in zip(self.all_keys, absolutes + percentages):
#             #checking here:
#             if key not in ['WC','WPS']:
#                 if ('%0.2f' % (counts[key] * 100)) != value:
#                     print("ERROR")
#                            
#             
#               
            liwc_resultlist.append(value)
        
        return liwc_resultlist
    
    #jwu added 06/2018    
    def return_percent_dict(self, counts):
        
        percent_dict=dict()
        percent_dict['WC']=counts['WC']
        percent_dict['WPS']=counts['WPS']
        
        for key in self.all_keys[2:]:
            percent_dict[key]=counts[key]*100
        
        return percent_dict

