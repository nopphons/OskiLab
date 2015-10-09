import sys
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument, PDFNoOutlines
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams, LTTextBox, LTTextLine, LTFigure, LTImage
from pdfminer.pdfdevice import PDFDevice
from pdfminer.pdfpage import PDFPage
import bisect
import re
import csv 
import pandas as pd

def with_pdf (pdf_doc, pdf_pwd, fn, *args):
    """Open the pdf document, and apply the function, returning the results"""
    result = None
    try:
        # open the pdf file
        fp = open(pdf_doc, 'rb')
        # create a parser object associated with the file object
        parser = PDFParser(fp)
        # create a PDFDocument object that stores the document structure
        doc = PDFDocument()
        # connect the parser and document objects
        parser.set_document(doc)
        doc.set_parser(parser)
        # supply the password for initialization
        doc.initialize(pdf_pwd)

        if doc.is_extractable:
            # apply the function and return the result
            result = fn(doc, *args)

        # close the pdf file
        fp.close()
    except IOError:
        # the file doesn't exist or similar problem
        pass
    return result

def _parse_toc (doc):
    """With an open PDFDocument object, get the table of contents (toc) data
    [this is a higher-order function to be passed to with_pdf()]"""
    toc = []
    try:
        outlines = doc.get_outlines()
        for (level,title,dest,a,se) in outlines:
            toc.append( (level, title) )
    except PDFNoOutlines:
        pass
    return toc


def get_toc (pdf_doc, pdf_pwd=''):
    """Return the table of contents (toc), if any, for this pdf file"""
    return with_pdf(pdf_doc, pdf_pwd, _parse_toc)


def to_bytestring (s, enc='utf-8'):
    """Convert the given unicode string to a bytestring, using the standard encoding,
    unless it's already a bytestring"""
    if s:
        if isinstance(s, str):
            return s
        else:
            return s.encode(enc)

def parse_lt_objs (lt_objs, page_number, images_folder, text=[]):
    """Iterate through the list of LT* objects and capture the text or image data contained in each"""
    text_content = [] 

    page_text = {} # k=(x0, x1) of the bbox, v=list of text strings within that bbox width (physical column)
    for lt_obj in lt_objs:
        if isinstance(lt_obj, LTTextBox) or isinstance(lt_obj, LTTextLine):
            # text, so arrange is logically based on its column width
            page_text = update_page_text_hash(page_text, lt_obj)
        elif isinstance(lt_obj, LTImage):
            # an image, so save it to the designated folder, and note it's place in the text 
            saved_file = save_image(lt_obj, page_number, images_folder)
            if saved_file:
                # use html style <img /> tag to mark the position of the image within the text
                text_content.append('<img src="'+os.path.join(images_folder, saved_file)+'" />')
            else:
                print >> sys.stderr, "error saving image on page", page_number, lt_obj.__repr__
        elif isinstance(lt_obj, LTFigure):
            # LTFigure objects are containers for other LT* objects, so recurse through the children
            text_content.append(parse_lt_objs(lt_obj.objs, page_number, images_folder, text_content))

    for k, v in sorted([(key,value) for (key,value) in page_text.items()]):
        # sort the page_text hash by the keys (x0,x1 values of the bbox),
        # which produces a top-down, left-to-right sequence of related columns
        text_content.append('\n'.join(v))

    return '\n'.join(text_content)

def _parse_pages (doc, images_folder):
    """With an open PDFDocument object, get the pages, parse each one, and return the entire text
    [this is a higher-order function to be passed to with_pdf()]"""
    rsrcmgr = PDFResourceManager()
    laparams = LAParams()
    device = PDFPageAggregator(rsrcmgr, laparams=laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, device)

    text_content = [] # a list of strings, each representing text collected from each page of the doc
    for i, page in enumerate(doc.get_pages()):
        interpreter.process_page(page)
        # receive the LTPage object for this page
        layout = device.get_result()
        # layout is an LTPage object which may contain child objects like LTTextBox, LTFigure, LTImage, etc.
        text_content.append(parse_lt_objs(layout.objs, (i+1), images_folder))

    return text_content


def get_pages (pdf_doc, pdf_pwd='', images_folder='/tmp'):
    """Process each of the pages in this pdf file and print the entire text to stdout"""
    print '\n\n'.join(with_pdf(pdf_doc, pdf_pwd, _parse_pages, *tuple([images_folder])))

def update_page_text_hash (h, lt_obj, pct=0.2):
    """Use the bbox x0,x1 values within pct to produce lists of associated text within the hash"""
    x0 = lt_obj.bbox[0]
    x1 = lt_obj.bbox[2]
    key_found = False
    for k, v in h.items():
        hash_x0 = k[0]
        if x0 >= (hash_x0 * (1.0-pct)) and (hash_x0 * (1.0+pct)) >= x0:
            hash_x1 = k[1]
            if x1 >= (hash_x1 * (1.0-pct)) and (hash_x1 * (1.0+pct)) >= x1:
                # the text inside this LT* object was positioned at the same
                # width as a prior series of text, so it belongs together
                key_found = True
                v.append(to_bytestring(lt_obj.get_text()))
                h[k] = v
    if not key_found:
        # the text, based on width, is a new series,
        # so it gets its own series (entry in the hash)
        h[(x0,x1)] = [to_bytestring(lt_obj.get_text())]
    return h

def add_article(articles, pgnum, agg_str, title, issue, year, month, author = '', text = '', subtitle = ''):
    """Add info into the dictionary called articles with keys = page number (pgnum) and
    val = dictionary with corresponding information"""
    articles[int(pgnum)] = {'Article Title (TOC)':agg_str}
    articles[int(pgnum)]['Start Page'] = pgnum
    articles[int(pgnum)]['Publication Title'] = title
    articles[int(pgnum)]['Issue Title'] = issue
    articles[int(pgnum)]['Year'] = year
    articles[int(pgnum)]['Month'] = month
    articles[int(pgnum)]['Article Author'] = author
    articles[int(pgnum)]['Article Text'] = text
    articles[int(pgnum)]['Article Subtitle'] = subtitle


###########################Main Work Flow Starts Here###########################
#input = 'Magazines/December 2014 FULL OCR.pdf'
input = sys.argv[1] # input from first arg command line
fp = open(input, 'rb') 
parser = PDFParser(fp) #parse the file

#followings are the template to use pdfminor package (cn be found online)
document = PDFDocument(parser) 
rsrcmgr = PDFResourceManager()
laparams = LAParams()
device = PDFPageAggregator(rsrcmgr, laparams=laparams)
interpreter = PDFPageInterpreter(rsrcmgr, device)
articles = {} #initial empty dictionary
book_index = 0 #initialize a parameter for pg number of book contents
for i, page in enumerate(PDFPage.create_pages(document)):
    i -= 1 #make i correspond to the real page number
    if book_index == 0 and (i == 2 or i == 4 or i == 6): #based on observation, the book content pg number is either 2,4, or 6
        interpreter.process_page(page)
          # receive the LTPage object for the page.
        layout = device.get_result()
        first_obj = layout._objs[0] #the first object of the page
        if (isinstance(first_obj, LTTextBox) or isinstance(first_obj, LTTextLine)) and 'FOOD' in first_obj.get_text().strip(): #condition to check if this page is a content page
            title = first_obj.get_text().strip() #if yes, let title = text in the first object
            book_index = i #set the book content page
            for issue_index in range(1,5): #get the issue text which will be somewhere in obj1 to obj5
                issue_obj = layout._objs[issue_index] 
                if issue_obj.y0 < 400:  #issue text is the first text below y = 400 level
                    full_issue = issue_obj.get_text().strip().split("\n")
                    issue = full_issue[0]
                    break
            for lt_obj in layout._objs: #get the year, month from the bottom left of the page
                if lt_obj.x1 <150 and lt_obj.y0 <34 and (isinstance(lt_obj, LTTextBox) or isinstance(lt_obj, LTTextLine)):
                    date = lt_obj.get_text().split(' ')
                    for j, alp in enumerate(date):
                        if alp.isdigit():
                            part = j
                            month = ''.join(date[:part]).strip()
                            year = ''.join(date[part:]).strip()
                            break
            second_text = " ".join(full_issue[1:]); count = 0 #sometime the issue text will contain the articles and their page number
            #count = counter of the number of objects, second_text = the rest of the text after the issue name
            for lt_obj in layout._objs[(issue_index+1):]: #get all the articles here 
                if isinstance(lt_obj, LTTextBox) or isinstance(lt_obj, LTTextLine):
                    txt = lt_obj.get_text().replace("\n", " ") 
                    words = txt.split(' ')
                    pgnum = words[0]; #page of the articles always starts at the beginning
                    if pgnum.isdigit() and int(pgnum) != book_index:
                        break
                    count += 1
                    second_text += " "
                    second_text += txt
            agg_str = ' '; bword = None; concat = False
            for word in second_text.split(' '):
                if word.isdigit() :
                    if int(word) < int(pgnum):
                        if not bword:
                            bword = word
                            concat = True
                        else: 
                            if concat:
                                word = ''.join([bword,word])
                    if int(word) > int(pgnum):
                        concat = False
                        pgnum = word
                        add_article(articles, pgnum, agg_str, title, issue, year, month)
                        agg_str = ''
                else: 
                    agg_str += word 
                    agg_str += ' '

            min_page = 0 #initialize the variable minimum page so far
            for lt_obj in layout._objs[(issue_index+1+count):]:
                if isinstance(lt_obj, LTTextBox) or isinstance(lt_obj, LTTextLine):
                    # text, so arrange is logically based on its column width
                    txts = lt_obj.get_text().split("\n")
                    for txt in txts:
                        txt = txt.replace("611", "61 T") #Hard-coded mistake when read text from pdf
                        words = txt.split(' ')
                        pgnum = words[0]; agg_str = ''
                        if pgnum.isdigit() and int(pgnum) != book_index and int(pgnum) > int(min_page): #basic check to make sure that page number is increasing
                            for word in words[1:]: #find all digits in the line (assume digits = page number)
                                if word.isdigit() and int(word) > int(pgnum):
                                    add_article(articles, pgnum, agg_str, title, issue, year, month)
                                    pgnum = word
                                    agg_str = ''
                                else: 
                                    agg_str += word 
                                    agg_str += ' '
                            add_article(articles, pgnum, agg_str, title, issue, year, month)
                            article_page = sorted(articles.keys())
                            min_page = pgnum
        
    if book_index != 0 and i > book_index and i > min(article_page):
        index = bisect.bisect(article_page, i)-1
        start_page = article_page[index]
        interpreter.process_page(page)
        text_page = []
        # receive the LTPage object for the page.
        layout = device.get_result()
        for lt_obj in layout._objs:
            if isinstance(lt_obj, LTTextBox) or isinstance(lt_obj, LTTextLine):
                text_page.append(lt_obj.get_text().strip().replace("\n", ""))
        articles[start_page]['Article Text'] += ' '.join(text_page)


#add the articles to the dataframe
df = pd.DataFrame(columns=["Publication Title","Year","Month","Issue Title","Article Title (TOC)"
    ,"Article Author","Article Subtitle","Article Text","Start Page"])
for i,j  in enumerate(article_page):
    data = articles[j]
    df.loc[i,'Publication Title'] = data["Publication Title"].encode('utf-8').strip()
    df.loc[i,'Year'] = data["Year"].encode('utf-8').strip()
    df.loc[i,'Month'] = data["Month"].encode('utf-8').strip()
    df.loc[i,'Issue Title'] = data["Issue Title"].encode('utf-8').strip()
    df.loc[i,'Article Title (TOC)'] = data["Article Title (TOC)"].encode('utf-8').strip()
    df.loc[i,'Article Author'] = data["Article Author"].encode('utf-8').strip()
    df.loc[i,'Article Subtitle'] = data["Article Subtitle"].encode('utf-8').strip()
    df.loc[i,'Article Text'] = data["Article Text"].encode('utf-8').strip()
    df.loc[i,'Start Page'] = data["Start Page"].encode('utf-8').strip()


#output = 'food_wine_april.csv'
output = sys.argv[2] #output name = second argument from command line
df.to_csv(output,header = True, encoding='utf-8', index = False) #write df to output csv file

#usage: python -i pdf2txt_gen.py Magazines/December 2014 FULL OCR.pdf food_wine_april.csv






