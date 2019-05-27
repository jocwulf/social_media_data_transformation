'''
Created on 22.09.2016

@author: jwulf

Please remember to configure dictionary locations in liwc file
Remember to manage language configs, i.e. primary language in function set language

TODOs
-enable restart
-catch errors

'''


from __future__ import division
from collections import Counter
from liwc import Liwc
import csv
from langid.langid import LanguageIdentifier, model
import glob, os, re
from multiprocessing import Pool
from datetime import datetime
import sys

def unicode_csv_reader(utf8_data, dialect=csv.excel, **kwargs):
    csv_reader = csv.reader(utf8_data, dialect=dialect, **kwargs)
    inloop = True
    while inloop == True:
        try:
            row= csv_reader.next()
            #print(csv_reader.line_num) 
            yield [unicode(cell, 'utf-8') for cell in row]
        except StopIteration:
            inloop = False
        except Exception as e:
                print("caught exception: %s  in line %s of file %s" % (e,str(csv_reader.line_num),filename))
                continue
        
#         try:
#             row= csv_reader.next()
#         for row in csv_reader:  
#         if csv_reader.line_num>350000: 
#             row = [c.replace('\x00', '') for c in row]
#             try:
#                 #print(str(csv_reader.line_num))
#                 yield [unicode(cell, 'utf-8') for cell in row]
#             except Exception as e:
#                 print("caught exception:")
#                 print(e)
#                 print(filename)
#                 numoferrors +=1
#                 continue
                


     
        
def liwcmine(filename_raw):
    try:
        print("starting to process file "+filename_raw)
        liwc_lexicon = Liwc()
        identifier = LanguageIdentifier.from_modelstring(model, norm_probs=True)
    
        filename = filename_raw.replace(".\\",'')
        filename_out = filename_raw.replace(".csv",'')
        
        fout=open("./liwc_scores/"+filename_out+"_liwc.csv","ab") 
        #header line
        #f = csv.reader(open(filename,"rb"), delimiter='\t')
        
        f = unicode_csv_reader(open(filename,"rb"),delimiter='\t')
            
        header_in = f.next()  
        colnum=len(header_in)
        header_list = header_in
        for key in liwc_lexicon.all_keys:
            header_list.append(key)
        header_list.append('language')
        header_list.append('lang_prob')
        header_list.append('has_mail')
        header_list.append('\r\n')
        header_out = '\t'.join(header_list)
        fout.write(header_out)
    
        #all other lines
        for line in f:
            try:
                liwc_lexicon.set_link(False)
                liwc_lexicon.reset_mail()
                fbtext = line[11]
                
                ### Language based Predeterimnation for Preselection:
                language_plus_prob = identifier.classify(fbtext)
                language = language_plus_prob[0]
                normalized_probability = language_plus_prob[1]
                
                fbstatustype = line[13]
                if fbstatustype == "link":
                    liwc_lexicon.set_link(True)
                
                full_counts = liwc_lexicon.summarize_document(fbtext, langinfo=language, langprob = normalized_probability)
                liwc_resultlist = liwc_lexicon.print_tabbed_summarization(full_counts)
                has_mail = int(liwc_lexicon.return_mail())
                linelist=line
                while len(linelist)<colnum:
                    linelist.append('')       
                linelist.extend(liwc_resultlist)
                linelist.append(language)
                linelist.append(str(normalized_probability))
                linelist.append(str(has_mail))
                linelist.append('\r\n')
                if len(linelist)==len(header_list):
                    line_out = '\t'.join(linelist)
                    fout.write(line_out.encode('utf-8'))
                else:
                    print("error:wrong outputlinelength. lineoutput:  "+'\t'.join(linelist))
            except Exception as e:
                print("caught exception: %s  in file %s" % (e,filename))
                continue
      
        print("finished to process file "+filename_raw)
        fout.close()
        return 0
    except:
        print("error, please drop file "+filename_raw)
        print("Unexpected error:"+str(sys.exc_info()[0]))
        return 1
        
###################################################################################################
####entry point which organizes multiprocessing
####################################################################################################
if __name__ == '__main__':
        
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname("__filename__")))         # Setzt Pfad gleich Ablagepfad der .py Datei
    files = glob.glob('./*feed*.csv')
    totalfiles = len(files)

    if not os.path.exists("liwc_scores"):
        os.makedirs("liwc_scores")
     
    #get the list of files which already are processed in order to skip them afterwords  
    files_done = glob.glob('./liwc_scores/*.csv')
    
    
    if os.name == 'nt':
        files_done = [filedone.split("\\")[1].split("_")[0] for filedone in files_done] #this windows
    else:
        files_done = [filedone.split("/")[2].split("_")[0] for filedone in files_done] #this linux

    files_formated = dict()
    if os.name == 'nt':
        for filename_raw in files:
            files_formated[filename_raw] = filename_raw.split("\\")[1].split("_")[0] #this windows
    else:
        for filename_raw in files:
            files_formated[filename_raw] = filename_raw.split("/")[1].split("_")[0] #this linux

    files_selected = [rawname for rawname,thefile in files_formated.iteritems() if ((thefile not in files_done))]
    
    pool = Pool(16)

    filecnt = 0
    dropcnt = 0
    errors = 0
    filetotal = len(files_selected)
    for dropcnt in pool.imap_unordered(liwcmine, files_selected):
        filecnt += 1
        errors += dropcnt
        print("%d of %d files processed at time %s" % (filecnt,filetotal,str(datetime.now()))) 
    print("finished with all  at "+str(datetime.now())) 
    print("number of errors:"+str(errors))      

        

