from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np
import random 
import csv

class pre_process(MRJob):
    id_text={}
    vector=[]
    voc_len=int()
    id_word=[]
    IDFS={}
    def steps(self):    # funtion for multiple mappers and reducers
        return [
            MRStep(mapper=self.mapper1, reducer=self.reducer1),
            MRStep(mapper=self.mapper2, reducer=self.reducer2),
            MRStep(mapper=self.mapper3, reducer=self.reducer3),
            MRStep(mapper=self.mapper4, reducer=self.reducer4)
        ]
    def getdata(self):
        temp=[]
        for i,j in self.id_text.items():
            temp.append(j)
        return temp
   
    def remove_stopwords(self,text):
        stopwords = {
    'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", 
    "you'll", "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 
    'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 
    'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 
    'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 
    'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 
    'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 
    'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 
    'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 
    'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 
    'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 
    'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 
    'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', 
    "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 
    'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', 
    "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 
    'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't",
    'able', 'about', 'above', 'according', 'accordingly', 'across', 'actually', 'after', 
    'afterwards', 'again', 'against', 'ain', 'all', 'allow', 'allows', 'almost', 'alone', 
    'along', 'already', 'also', 'although', 'always', 'am', 'among', 'amongst', 'an', 'and', 
    'another', 'any', 'anybody', 'anyhow', 'anyone', 'anything', 'anyway', 'anyways', 'anywhere', 
    'apart', 'appear', 'appreciate', 'appropriate', 'are', 'aren', "aren't", 'around', 'as', 
    'aside', 'ask', 'asking', 'associated', 'at', 'available', 'away', 'awfully', 'b', 'be', 
    'became', 'because', 'become', 'becomes', 'becoming', 'been', 'before', 'beforehand', 
    'behind', 'being', 'believe', 'below', 'beside', 'besides', 'best', 'better', 'between', 
    'beyond', 'both', 'brief', 'but', 'by', 'c', 'came', 'can', 'cannot', 'cant', "can't", 
    'cause', 'causes', 'certain', 'certainly', 'changes', 'clearly', 'co', 'com', 'come', 
    'comes', 'concerning', 'consequently', 'consider', 'considering', 'contain', 'containing', 
    'contains', 'corresponding', 'could', 'couldn', "couldn't", 'course', 'currently', 'd', 
    'definitely', 'described', 'despite', 'did', 'didn', "didn't", 'different', 'do', 'does', 
    'doesn', "doesn't", 'doing', 'don', "don't", 'done', 'down', 'downwards', 'during', 'e', 
    'each', 'edu', 'eg', 'eight', 'either', 'else', 'elsewhere', 'enough', 'entirely', 'especially', 
    'et', 'etc', 'even', 'ever', 'every', 'everybody', 'everyone', 'everything', 'everywhere', 
    'ex', 'exactly', 'example', 'except', 'f', 'far', 'few', 'fifth', 'first', 'five', 'followed', 
    'following', 'follows', 'for', 'former', 'formerly', 'forth', 'four', 'from', 'further', 
    'furthermore', 'g', 'get', 'gets', 'getting', 'given', 'gives', 'go', 'goes', 'going', 
    'gone', 'got', 'gotten', 'greetings', 'h', 'had', "hadn't", 'happens', 'hardly', 'has', 
    'hasn', "hasn't", 'have', 'haven', "haven't", 'having', 'he', 'hello', 'help', 'hence', 
    'her', 'here', 'hereafter', 'hereby', 'herein', 'hereupon', 'hers', 'herself', 'hi', 
    'him', 'himself', 'his', 'hither', 'hopefully', 'how', 'howbeit', 'however', 'i', 'ie', 
    'if', 'ignored', 'immediate', 'in', 'inasmuch', 'inc', 'indeed', 'indicate', 'indicated', 
    'indicates', 'inner', 'insofar', 'instead', 'into', 'inward', 'is', 'isn', "isn't", 'it', 
    "it'd", "it'll", 'itself', 'its', 'itself', 'j', 'just', 'k', 'keep', 'keeps', 'kept', 
    'know', 'knows', 'known', 'l', 'last', 'lately', 'later', 'latter', 'latterly', 'least', 
    'less', 'lest', 'let', "let's", 'like', 'liked', 'likely', 'little', 'look', 'looking', 
    'looks', 'ltd', 'm', 'mainly', 'many', 'may', 'maybe', 'me', 'mean', 'meanwhile', 'merely', 
    'might', 'more', 'moreover', 'most', 'mostly', 'much', 'must', 'my', 'myself', 'n', 'name', 
    'namely', 'nd', 'near', 'nearly', 'necessary', 'need', 'needs', 'neither', 'never', 
    'nevertheless', 'new', 'next', 'nine', 'no', 'nobody', 'non',"*","_","-","=" 'none','\n','an',"to","be","of"}
    #text=str(text)
        new_text = ""
        for word in text.split():
            if word.lower() not in stopwords:
               new_text += word + " "
        return new_text.strip()
        
    
    
    def mapper1(self, _, line):       #use command python3 new.py sample.csv to run 
        row=next(csv.reader([line]))
        if len(row)>=4:
           col1,col2,col3,col4=row[:4]
           if len(col4)>5:
              col4=self.remove_stopwords(col4)
              self.id_text[col1]=col4
              #col4=col4.strip().split()
              yield col1, col4
              
    def reducer1(self, key, values):    #making list of unique words along with the id
        
        processed_values = list(values)
        unique_words = []  # List to store unique tuples
        temp = str(processed_values).split()
        for x, i in enumerate(temp):
            if i not in unique_words:
               xv=random.randint(0,38274824)
               unique_words.append([xv, i])
        self.id_word=unique_words  
        self.voc_len=len(unique_words)
    # Convert generator to list
    # yield key, processed_values
        for i in self.id_word:
            yield processed_values, i

    def mapper2(self, _, line):
      #  x = self.getdata()
    # Log the value of x to check if it's obtained correctly
        
        yield _,line
    # Convert the list to a tuple
  
        
    def reducer2(self, _, line):
        IDF={}
        processed_values = list(line)

        for i in processed_values:
            x=i[1]
            for chk in set(_):
                if x in chk:
                    if x not in IDF:
                        IDF[x]=1
                    else:
                        IDF[x]+=1
        self.IDFS.update(IDF)
        for key ,value in IDF.items():
            yield key, value

    def mapper3(self,word,IDF):
        yield word,[IDF]

    def reducer3(self, word, IDF):
        l = []
        for i in word:
            for j, z in self.id_text.items():
                if isinstance(z, int):  # Check if z is an integer
                    z = [z]  # Convert z to a list containing the integer
                z = set(z)
                if i in z:
                    self.id_text[j] = self.IDFS.get(i, 1)
        for i,v in self.id_text.items():
            x=random.randint(1,19)
            p=random.randint(1,5)
           
            arr=np.zeros(20)
            arr[x]=p
            x=random.randint(1,19)
            arr[x]=p
            l.append(arr.tolist())
        self.vector=l 
        yield word, l


    def ran(self):
        c=random.randint(0,19)
        return c
    
    def mapper4(self,word,vector):
        x=self.id_text.keys()
        x=[x]
        yield x, vector
    def reducer4(self,key,vector):
        query="this is some text"
        l=[]
        for i in range(0,len(query)):
            x=random.randint(1,4)
            l.append(x)
        q_vector=np.zeros(19)
        p=random.randint(0,19)
        for i in range(len(l)):
            z=self.ran()
            q_vector[z]=l[i]
        q_sum=sum(q_vector)
        for i in vector:
            dot=q_vector.dot(i)
            if dot>0.3:
                yield key,dot
            else:
                pass

if __name__ == '__main__':
    pre_process.run()

