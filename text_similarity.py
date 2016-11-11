# -*- coding: utf-8 -*-
from gensim import corpora,models,similarities
import logging
import re
import numpy
import jieba
import os
import sys
import MySQLdb
import MySQLdb.cursors
from twisted.enterprise import adbapi
reload(sys)
sys.setdefaultencoding('utf-8')
def document_generate():
	documents = []

	#get stop words
	stop_words = []
	fs = open('stop_words_zh.txt','r')
	fs_lines = fs.readlines()
	for fs_line in fs_lines:
		stop_words.append(unicode(fs_line.strip(),'utf-8'))
	#print "stop_words", stop_words
	
	#get term list
	term_list = []
	term_dict = {}
	cnt = 0
	# ft = open('corpus\\' + u'C语言_en' + '\\' +'term_list.txt','r')
	# ft_lines = ft.readlines()
	ft_lines = os.listdir('corpus\\' + u'summary5')
	for ft_line in ft_lines:
		term = re.compile('.txt').sub('', ft_line)
		#term = ft_line.strip()
		#term_dict[cnt] = term
		term_dict[cnt] = re.sub('_all', '', term)
		cnt = cnt + 1
		path = 'corpus\\' + u'summary5' + '\\' + term + '.txt'
		fp = open(path,'r')
		words = fp.read()
		for i in ('(',')','\n','.',',','\"','[',']','{','}','?',':','\\','/',"'"):
			words = words.replace(i,'')
		words = re.sub(r'\s+', '', words).lower()
		# words_list = words.split(' ')
		words_cut = ' '.join(jieba.cut(words))
		words_list = words_cut.split(' ')
		# print type(words_list)
		# print type(words_list[0])
		# print type(stop_words)
		# print type(stop_words[380])
		# print "words_list[0] = ", words_list[0]
		# print "stop_words = ", stop_words
		# print "words_list = ", words_list
		non_stop_words = ''
		for word in words_list:
			if word not in stop_words:
				non_stop_words = non_stop_words + word + ' '
		documents.append(non_stop_words)
	return documents,term_dict

if __name__ == '__main__':
	logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level = logging.INFO)

	docs,term_dict = document_generate()
	print 'test = ', term_dict
	#print doc[0]
	texts = [[word for word in doc.split()] for doc in docs]
	#print texts[0]
	dictionary = corpora.Dictionary(texts)
	#print dictionary.token2id
	corpus = [dictionary.doc2bow(text) for text in texts]
	tfidf = models.TfidfModel(corpus)
	corpus_tfidf = tfidf[corpus]
	
	#lsi
	# lsi = models.LsiModel(corpus_tfidf,id2word=dictionary,num_topics=200)
	# lsi.print_topics(100)
	# corpus_lsi = lsi[corpus_tfidf]

	#lda
	lda = models.LdaModel(corpus_tfidf,id2word=dictionary,num_topics=200)
	lda.print_topics(2)
	corpus_lda = lda[corpus_tfidf]

	#index = similarities.MatrixSimilarity(lsi[corpus])
	index = similarities.MatrixSimilarity(lda[corpus])

	# doc1 = texts[0]
	# doc1_bow = dictionary.doc2bow(doc1)
	# doc1_lsi = lsi[doc1_bow]
	# print "doc1_lsi ", doc1_lsi
	# sims1 = index[doc1_lsi]
	# print sims1

	# doc2 = texts[6]
	# doc2_bow = dictionary.doc2bow(doc2)
	# doc2_lsi = lsi[doc2_bow]
	# print "doc2_lsi ", doc2_lsi
	# sims2 = index[doc2_lsi]
	# print sims2
	# 
	#f = open('output' + '\\' + u'百度百科_C语言' + '\\' + "simlarity_topic=200_lda.csv",'w')
	similar_dict = {}

	term_number = len(texts)
	for i in range(0,term_number):
		for j in range(i+1,term_number):
			doc1 = texts[i]
			doc1_bow = dictionary.doc2bow(doc1)
			
			# doc1_lsi = lsi[doc1_bow]
			# sims1 = index[doc1_lsi]			
			doc1_lda = lda[doc1_bow]
			sims1 = index[doc1_lda]

			doc2 = texts[j]
			doc2_bow = dictionary.doc2bow(doc2)

			# doc2_lsi = lsi[doc2_bow]
			# sims2 = index[doc2_lsi]
			doc2_lda = lda[doc2_bow]
			sims2 = index[doc2_lda]
			
			cosV12 = numpy.dot(sims1,sims2)/(numpy.linalg.norm(sims1)*numpy.linalg.norm(sims2))

			similar_dict[term_dict[i] + ',' + term_dict[j]] = cosV12

			#f.write(re.compile('_all').sub('', term_dict[i]) + ',' + re.compile('_all').sub('', term_dict[j]) + ',' + str(cosV12) + '\n')
	sorted_similar = sorted(similar_dict.iteritems(), key=lambda d:d[1], reverse=True)
	#print sorted_similar
	

	#print type(sorted_similar[0])
	#f = open("simlarity.csv",'w')
	fw = open('output' + '\\' + u'summary5' + '\\' + "relation_lda200000000.csv",'w')
	for sim in sorted_similar:
		fw.write(sim[0] + ',' + str(sim[1]) + '\n')