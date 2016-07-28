###
#
# migration.py
#
# A script to migrate csv files into Wordpress
#
# classes:  Migration
#
#
#
###

import pdb
import optparse
import csv
import re
from unidecode import unidecode
#import phpserialize
from datetime import datetime
from db import Db
from os import walk


class Migration:
# primary class for handling the data import / migration

	def __init__(self, options):

		# if the user has specified the credentials of the database to migrate to
		if options.db_user or options.db_pass or options.db_name:
			if options.db_user and options.db_pass and options.db_name:
				credentials = {
						'user': options.db_user,
						'pass': options.db_pass,
						'db': options.db_name
						}
				self.db = Db( credentials )
			else:
				print('Need db_user, db_pass, and db_name or omit all to use .creds.json file')

		# otherwise, use the default from .creds.json
		else:
			self.Db = Db()

		# if overwrite is set to true, call reset_db
		if options.overwrite:
			self.Db.reset_db( 'full' )

		# assign leftover options to self
		if options.csv_fname:
			self.csv_fname = options.csv_fname
		else:
			self.csv_dir = options.csv_dir

		# titles list to ensure unique titles
		self._post_titles = []

		return

	def migrate_posts(self):
	# migrate all posts and post_meta

		# first get the posts from the specified file, or if no file, from the dir
		if hasattr(self, 'csv_fname'):
			self._fetch_posts(self.csv_fname)

		# get all posts from csvs in the specified dir (defaults to .)
		else:
			for dirpath, dirnames, filenames in walk(self.csv_dir):
				for fname in filenames:
					if fname.endswith(".csv"):
						self._fetch_posts( dirpath + fname)

		# then, finally, write all the posts to the wp db
		self._write_posts()

		return

	def _fetch_posts(self, fname):
	# get posts data from origin

		if not hasattr(self, 'posts_to_migrate'):
			self.posts_to_migrate = []

		# open and process the CSV file
		with open(fname, newline='') as csvfile:
			posts = csv.reader(csvfile, delimiter=',', quotechar='"')

			# get all the necessary meta information about the posts
			for post in posts:

				post_obj = {

						# their id system
						# not migrated
						'_scrolls_id' : post[0],

						# type data for wordpress
						# string
						'type' : post[1],

						# repository
						# string
						'repository' : post[2],

						# Lib. city
						# string
						'repository_city' : post[3],

						# Lib. nat
						# string
						'repository_nation' : post[4],

						# Lib. nat
						# string
						'repository_name_and_city' : post[5],

						# blank
						#'_blank' : post[6],

						# Lib. lat
						# string
						'lib_lat' : post[7],

						# Lib. lon
						# string
						'lib_lon' : post[8],

						# ref number / shelfmark
						# string
						'shelfmark' : post[9],

						# year from and to
						# int
						'date_start' : post[10],
						# int
						'date_end' : post[11],

						# date quality
						# string
						'date_quality' : post[12],

						# provenance
						# string
						'provenance' : post[13],

						# provenance latitude
						# string
						'prov_lat' : post[14],

						# provenance longitude
						# tax
						'prov_lon' : post[15],

						# length / width
						# string
						'length' : post[16],
						'width' : post[17],

						# number of pieces
						# int
						'number_of_pieces' : post[18],

						# orientation
						# string
						'orientation' : post[19],

						# completed
						# string
						'completed' : post[20],

						# languages
						# string
						'language_1' : post[21],
						'language_2' : post[22],

						# contents
						# string
						'contents' : post[23],

						# description
						# string
						'description' : post[24],

						# bibliography
						# string
						'bibliography' : post[25].replace(";", "<br/>"),

						# editor initials
						# string
						'editor_initals' : post[26],

						# online images link
						# string
						'online_images' : post[27],

						# online bib record
						# string
						'online_bibliography_record' : post[28],

					}


				# generate the title and post_name based on the values in the post_obj
				post_obj['title'] = self._generate_title(post_obj)
				post_obj['post_name'] = self._to_postname(post_obj['title'])

				post_obj['meta_title'] = post_obj['title']

				# add to orig posts list
				self.posts_to_migrate.append(post_obj)

		return

	def _write_posts(self):
	# save posts data to new db

		db, cursor = self.Db.db()
		timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

		count = 0
		print(' -- Migrating posts . . . ')

		for post in self.posts_to_migrate:


			sql = """INSERT INTO wp_posts
					( post_author, post_content, post_title, post_status, post_type, post_name, post_date, post_date_gmt, post_modified, post_modified_gmt )
				 VALUES
					( 1, '', %s, 'publish', 'scroll', %s, %s, %s, %s, %s )"""

			cursor.execute( sql, (
					post['title'],
					post['post_name'],
					timestamp,
					timestamp,
					timestamp,
					timestamp
				))

			if count >= 1000 and count % 100 == 0:
				print(' -- - ', count, 'posts . . . ')

			if count == len(self.posts_to_migrate) - 1:
				print(' -- - Migrated', count, 'posts')

			count += 1

		db.commit()
		db.close()

		return


	def migrate_meta(self):
	# migrate posts meta based on terms and posts in db

		"""
		-------------------------------------------
		"""

		db, cursor = self.Db.db()
		postmeta_debugging = []

		count = 0
		print(' -- Migrating postmeta . . . ')

		for post in self.posts_to_migrate:

			# first lookup the wp post id of the post
			post_id = self._get_post_wp_id(post)
			post['wp_post_id'] = post_id

			# if we haven't migrated this one yet
			if post_id not in postmeta_debugging:
				# add it to the list of completed post_ids
				postmeta_debugging.append(post_id)

				for key, value in post.items():
					if key not in ['title', 'post_name']:
						try:
							self._add_meta(cursor, post_id, key, value)
						except:
							pdb.set_trace()

					# well, add the taxonomy relationships here for now
					"""
					if key in ['languages', 'orientation', 'types']:
						if len(value) >= 1:
							if isinstance(value, list):
								for item in value:
									try:
										self._add_term_relationship(cursor, post_id, item)
									except:
										pdb.set_trace()
							else:
								try:
									self._add_term_relationship(cursor, post_id, value)
								except:
									pdb.set_trace()

					"""

				# and finally,
				# print some friendly command line information
				if count >= 100 and count % 100 == 0:
					print(' -- - ', count, 'posts postmeta added . . . ')
				if count == len(self.posts_to_migrate) - 1:
					print(' -- - Migrated', count, 'posts postmeta')

				count += 1

		#commit/close
		db.commit()
		db.close()

		return

	def _set_repository_relationship(self, post_obj, csv_obj):
	# establish the relationship between the repository and the post
		repo = post_obj['repository']
		repo_value = ''

		# check if it exists
		if len(repo) > 0:
			db, cursor = self.Db.db()
			timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
			title = self._make_repository_title(repo, csv_obj[2])
			postname = self._to_postname(title)

			sql = "SELECT ID FROM wp_posts WHERE post_title = %s AND post_type = 'repository'"
			cursor.execute( sql, (title) )
			results = cursor.fetchall()

			if len(results) == 0:

				sql = """INSERT INTO wp_posts
						( post_author, post_content, post_title, post_status, post_type, post_name, post_date, post_date_gmt, post_modified, post_modified_gmt )
					 VALUES
						( 1, '', %s, 'publish', 'repository', %s, %s, %s, %s, %s )"""

				cursor.execute( sql, (
						title,
						postname,
						timestamp,
						timestamp,
						timestamp,
						timestamp
					))
				db.commit()

				sql = "SELECT ID FROM wp_posts WHERE post_title = %s AND post_type = 'repository'"
				cursor.execute( sql, (title) )
				results = cursor.fetchall()

				repo_id = results[0][0]

				nation = self._lookup_repo_country(csv_obj[3])
				nation = self._lookup_tax_term('country', nation)
				self._add_meta(cursor, repo_id, 'nation', nation)
				self._add_meta(cursor, repo_id, 'city', csv_obj[2])


			else:
				repo_id = results[0][0]

			repo_value = phpserialize.dumps([repo_id])


		return repo_value

	def _make_repository_title(self, name, city):
	# do the special repository title
	# if the name contains "unknown" or "private", append city to name
		title = ''

		if "Unknown" in name or "unknown" in name or "Private" in name or "private" in name:
			title = name + " - " + city
		else:
			title = name

		return title

	def _lookup_repo_country(self, initials):

		country = ""

		if initials == "BE":
			country = "Belgium"
		elif initials == "US":
			country = "United States"
		elif initials == "GB":
			country = "Great Britain"
		elif initials == "DE":
			country = "Germany"
		elif initials == "FR":
			country = "France"
		elif initials == "IT":
			country = "Italy"
		elif initials == "NL":
			country = "Netherlands"
		elif initials == "UK":
			country = "United Kingdom"
		elif initials == "Unknown":
			country = "Unknown"
		elif initials == "AT":
			country = "Austria"
		elif initials == "SP":
			country = "Spain"
		elif initials == "CH":
			country = "Switzerland"
		elif initials == "NZ":
			country = "New Zealand"
		elif initials == "DK":
			country = "Denmark"
		elif initials == "NY":
			country = "United States" #grrr
		elif initials == "ES":
			country = "Spain" #double grr
		elif initials == "PL":
			country = "Poland"
		elif initials == "RU":
			country = "Russia"

		return country

	def _lookup_tax_term(self, taxonomy, value):
	# lookup the taxonomy term from wordpress for the value

		term_id = 0
		db, cursor = self.Db.db()


		# standardize some tax values with wordpress
		if value == "ca":
			value = "Circa"
		elif value == "" and taxonomy == "quality":
			value = "Exact"
		elif value == "H" or value == "h":
			value = "Horizontal"
		elif value == "V" or value == "v":
			value = "Vertical"

		if isinstance(value, list):
			# a predictably small list; this'll work.
			if len(value) == 1:
				sql = "SELECT term_id FROM wp_terms WHERE name = %s AND term_id IN (SELECT term_id FROM wp_term_taxonomy WHERE taxonomy = %s)"
				cursor.execute( sql, (value[0], taxonomy) )
			else:
				sql = "SELECT term_id FROM wp_terms WHERE name IN (%s, %s) AND term_id IN (SELECT term_id FROM wp_term_taxonomy WHERE taxonomy = %s)"
				cursor.execute( sql, (value[0], value[1], taxonomy) )

		else:
			sql = "SELECT term_id FROM wp_terms WHERE name = %s AND term_id IN (SELECT term_id FROM wp_term_taxonomy WHERE taxonomy = %s)"
			cursor.execute( sql, (value, taxonomy) )

		results = cursor.fetchall()

		db.close()

		term_id = []
		for result in results:
			term_id.append(result[0])

		if len(term_id) < 1:
			term_id = ''
		elif len(term_id) == 1:
			term_id = term_id[0]

		# some non-pythonic making python happy
		if isinstance(term_id, int):
			term_id = str(term_id) #jokes!

		return term_id

	def _get_post_wp_id(self, post):
	# return the wordpress id of the newly inserted post

		wp_id = 0
		db, cursor = self.Db.db()

		sql = "SELECT ID FROM wp_posts WHERE post_title = %s"
		cursor.execute( sql, (post['title']) )
		wp_id = cursor.fetchall()

		db.close()

		if len(wp_id) > 1:
			print("Error (warning): more than one result returned for the wp post id")
			print(post['title'])
			print(wp_id)

		return wp_id[0][0]


	def _add_meta(self, cursor, post_id, meta_key, meta_value):
	# add a meta value to the wordpress database

		sql = """INSERT INTO wp_postmeta
				( post_id, meta_key, meta_value )
			 VALUES
				( %s, %s, %s )"""

		cursor.execute( sql, (
				post_id,
				meta_key,
				meta_value
			))
		return

	def _add_term_relationship(self, cursor, post_id, term_taxonomy_id):
	# add a meta value to the wordpress database

		sql = """INSERT INTO wp_term_relationships
				( object_id, term_taxonomy_id, term_order )
			 VALUES
				( %s, %s, 0 )"""

		cursor.execute( sql, (
				post_id,
				term_taxonomy_id
			))

		return

	def _generate_title(self, post):

		iterator = 0
		if len(post["shelfmark"]) == 0:
			post["shelfmark"] = "No shelfmark"

		title = ""

		if len(post["repository_city"].strip()):
			title = post["repository_city"]

		if len(post["shelfmark"].strip()):
			title += ", " + post["shelfmark"]

		pre_uniquify_title = title
		while title in self._post_titles:
			iterator+=1
			title = pre_uniquify_title + ' ' + str(iterator)

		self._post_titles.append(title)

		return title

	def _to_postname(self, title):
	# generate a Wordpress friendly postname in the same manner that wordpress would

		post_name = unidecode(title)
		post_name = post_name.replace(' ', '-').lower()
		post_name = post_name.replace(':', '')
		post_name = post_name.replace('(', '')
		post_name = post_name.replace(')', '')
		post_name = post_name.replace('.', '-')
		post_name = post_name.replace(',', '')

		while "--" in post_name:
			post_name = post_name.replace('--', '-')

		return post_name






def main():
# main script execution


	# parse options for csv, migration type, database connection credentials, and if the program should overwrite or not
	parser = optparse.OptionParser()
	parser.add_option("-c", "--csv", dest="csv_fname", help="The filename (or full path) of the CSV to import data from", default="data.csv")
	parser.add_option("-t", "--type", dest="migration_type", help="posts (Wordpress Posts), meta (Postmeta), or full (Posts and Postmeta)", default='full')
	parser.add_option("-u", "--db_user", dest="db_user", help="Database username for migration target", default=False)
	parser.add_option("-p", "--db_pass", dest="db_pass", help="Database password for migration target", default=False)
	parser.add_option("-d", "--db_name", dest="db_name", help="Database name for migration target", default=False)
	parser.add_option("--overwrite", dest="overwrite", help="Should the migration drop existing posts and import or simply import the posts?", default=True)
	parser.add_option("-r", "--csv_dir", dest="csv_dir", help="Import all csv files from the specified directory (defaults to . )", default='.')
	(options, args) = parser.parse_args()

	print('-------------------------------------')
	print('Beginning Scrolls import:')
	print('-------------------------------------')

	migration = Migration( options )

	if options.migration_type == 'posts':
		migration.migrate_posts()
	elif options.migration_type == 'meta':
		migration.migrate_meta()
	elif options.migration_type == 'full':
		migration.migrate_posts()
		migration.migrate_meta()





if __name__ == "__main__":
	main()
