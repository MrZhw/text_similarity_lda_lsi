#!usr/bin/python
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
from twisted.internet import reactor
reload(sys)
sys.setdefaultencoding('utf-8')

class relation_extract(object):
	def __init__(self, domain, db , user, passwd, host, port):
		self.domain = domain.decode('gbk')
		self.db = db.decode('gbk')
		self.user = user.decode('gbk')
		self.passwd = passwd.decode('gbk')
		self.host = host.decode('gbk')
		self.port = int(port)
		self.dbpool = adbapi.ConnectionPool('MySQLdb',
			db = self.db,
			user = self.user,
			passwd = self.passwd,
			cursorclass = MySQLdb.cursors.DictCursor,
			charset = 'utf8',
			use_unicode = True,
			host = self.host,
			port = self.port,
		)

		self.docs = []
		self.index2term_dict = {}
		self.term2index_dict = {}
		self.termId2termName = {}
		self.cutted_documents = []

		# self.threshold = float(threshold)

	def _conditional_select(self):
		sql = "select * from spider_" + self.domain + "_text"
		d = self.dbpool.runQuery(sql).addCallback(self.printResult)
		reactor.callLater(4, reactor.stop)
		reactor.run()

	def printResult(self, lines):
		if lines:
			index = 0
			for line in lines:
				termId = line['TermID']
				content = line['FragmentContent']
				termName = line['TermName']
				if termId not in self.term2index_dict:
					self.docs.append(content)
					self.term2index_dict[termId] = index
					self.index2term_dict[index] = termId
					self.termId2termName[termId] = termName
					index += 1
				else:
					self.docs[self.term2index_dict[termId]] += content

	def handle_error(self, failure):
		logging.error(failure)

	def doc_cut(self):
		stop_words = []
		fs = open('D://stop_words_zh.txt','r')
		fs_lines = fs.readlines()
		for fs_line in fs_lines:
			stop_words.append(unicode(fs_line.strip(),'utf-8'))
		for doc in self.docs:
			words = doc
			for i in ('(',')','\n','.',',','\"','[',']','{','}','?',':','\\','/',"'"):
				words = words.replace(i,'')
			words = re.sub(r'\s+', '', words).lower()
			words_cut = ' '.join(jieba.cut(words))
			words_list = words_cut.split(' ')
			non_stop_words = ''
			for word in words_list:
				if word not in stop_words:
					non_stop_words = non_stop_words + word + ' '
			self.cutted_documents.append(non_stop_words)

	def text_relation(self):
		try:

			logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level = logging.INFO)

			term_dict = self.index2term_dict

			texts = [[word for word in doc.split()] for doc in self.cutted_documents]
			dictionary = corpora.Dictionary(texts)
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
					similar_dict[str(self.index2term_dict[i]) + ',' + str(self.index2term_dict[j])] = cosV12

			sorted_similar = sorted(similar_dict.iteritems(), key=lambda d:d[1], reverse=True)
			return sorted_similar
		except Exception, e:
			print e

	def _conditional_insert(self, records):
		maxNum = len(self.cutted_documents) * 5
		maxNum = maxNum if maxNum < len(records) else len(records)
		conn = None
		for record in records[:maxNum]:
			# if  record[1] < self.threshold:
			#  	break
			className = self.domain
			termId = record[0].split(',')
			startId = int(termId[0])
			startName = self.termId2termName[startId]
			endId = int(termId[1])
			endName = self.termId2termName[endId]
			confidence = record[1]
			conn = MySQLdb.connect(host=self.host, db=self.db, user=self.user, passwd=self.passwd,  port=self.port, charset='utf8')

			cur = conn.cursor()
			sql = "insert into dependency values (%s, %s, %s, %s, %s, %s)"
			try:
				cur.execute(sql,(className, str(startName), startId, str(endName), endId, confidence))
				conn.commit()
			except :
				conn.rollback()
			finally:
				conn.close()

if __name__ == '__main__':

	print sys.argv[0],type(sys.argv[1]),type(sys.argv[3]),type(sys.argv[6])
	try:
		realt = relation_extract(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
		realt._conditional_select()
		if len(realt.docs) > 0:
			realt.doc_cut()
			sorted_similar = realt.text_relation()
			realt._conditional_insert(sorted_similar)
		else:
			print "No record found"
	except Exception, e:
		print e