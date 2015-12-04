'''
Created on Oct 6, 2015

@author: Anil Singh

'''

import sys

def new_list(L):
    return [a for a in L]
    

def clean_text(text, splitChar=','):
    '''
    Author: Anil Singh, Oct 6, 2015
    
    Parameters   : text: Text string to be cleaned, splitChar = Character to split the string
    Return      : Cleaned String, List of clean substrings split at splitChar 
    
    Description: Perform cleanup of text strings obtained after sqlite queries.
    
     1. remove unicode sentinetls u'xxx'
     2. remove (, ) characters 
     3. remove (\) characters
     4. under development
     
    '''
    text = text.strip()
    ##Getting rid of unicode sentinel
    text = text.replace('u\'','')
    ##Following Characters mess-up with SQL
    text = text.replace('\'','')
    text = text.replace('(','')
    text = text.replace(')','')
    text = text.replace('.','')
    text = text.replace(' ','')
    text = text.replace('%','')
    text = text.replace('-','')
    ##print "clean_text",text
    return text, text.split(splitChar)

def clean_string(text,replaceHyphen=True):
    try:
        text = text.strip()
        ##Getting rid of unicode sentinel
        text = text.replace('u\'','')
        ##Following Characters mess-up with SQL
        text = text.replace('\'','')
        text = text.replace('(','')
        text = text.replace(')','')
        text = text.replace('.','')
        ##text = text.replace(' ','')
        text = text.replace('%','')
        if(replaceHyphen==True):
            text = text.replace('-','')
        text =text.replace('"','')
        ##print "clean_text",text
        ##Removing all non-ascii characters.
        text = ''.join([i for i in text if ord(i)<128])
        return text
    except Exception, err:
        print_error(err, 'pytools.clean_string')

def clean_split_string(text,splitChar=','):
    text = clean_string(text)
    return text.split(splitChar)


def straight_line(character,length):
    return (''.join([character]*length))


def cosmetic_line(numCol, widCol):
    unitString = straight_line('-', widCol)+'+'
    return ''.join([unitString]* numCol)

def print_tuple(vals,colWid=20):
    i =0
    frmt = ''
    for val in vals:
        frmt = frmt+'{'+str(i)+':'+str(colWid)+'s}'
        i = i+1
    ##Unpacking the elements of tuple into arguments.
    ##print("First item: {:d}, second item: {:d} and third item: {:d}.".format(*tuple))
    ##from : http://stackoverflow.com/questions/15181927/new-style-formatting-with-tuple-as-argument
    strVal = map(str,vals)
    print (frmt.format(*strVal))
   

def print_error(err,place=''):
    sys.stderr.write(('ERROR ('+place+'):  %s\n') % str(err))
    

def month_enum(token):
    return {
            'January':'1',
            'February':'2',
            'March':'3',
            'April':'4',
            'May':'5',
            'June':'6',
            'July':'7',
            'August':'8',
            'September':'9',
            'October':'10',
            'November':'11',
            'December':'12'
        }[token]