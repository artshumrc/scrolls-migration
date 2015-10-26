###
#
# db.py
#
# Useful functions to help with connecting to the database
#
# classes:  Db
#
#
###

import pymysql 

class Db:
	
	def __init__(self, credentials=False):

		self.credentials = credentials
		if not credentials:
			from creds import creds 
			self.credentials = creds()

		return

	# reset the db to select state based on reset value
	def reset_db(self, reset):

		print( ' -- Resetting the database . . . ' )

		db, cursor = self.db() 

		if reset == 'posts':
			
			self._delete_posts(cursor)

		if reset == 'meta':
			
			self._delete_meta(cursor)

		elif reset == 'full':

			self._delete_meta(cursor)
			self._delete_posts(cursor)

		# commit and close db
		db.commit()
		db.close()
		return

	# delete the existing posts from the database
	def _delete_posts(self, cursor):
		print( ' -- - Dropping existing posts . . . ' )

		cursor.execute("DELETE FROM wp_posts WHERE post_type IN ('scroll', 'scrolls', 'repository') OR post_status = 'auto-draft';")
		try:
			cursor.execute("ALTER TABLE wp_posts AUTO_INCREMENT = 100;")
		except:
			print( ' -- could not reset wp_posts auto_increment value' )
			pass
		return

	# delete the postmeta values from the database
	def _delete_meta(self, cursor):

		print( ' -- - Dropping existing postmeta . . . ' )

		cursor.execute("DELETE FROM wp_postmeta WHERE post_id IN (SELECT ID from wp_posts where post_type IN ('scroll', 'scrolls', 'repository') );")
		cursor.execute("DELETE FROM wp_term_relationships WHERE object_id IN (SELECT ID from wp_posts where post_type IN ('scroll', 'scrolls', 'repository') );")

		return

	# connect to mysqldb and return cursor
	def db(self):

		db = pymysql.connect(host="127.0.0.1", user=self.credentials['user'], passwd=self.credentials['pass'], db=self.credentials['db'], port=3306, charset='utf8')
		
		if (db):
			cursor = db.cursor()

			# Force unicode with each connection
			cursor.execute('SET NAMES utf8;') 
			cursor.execute('SET CHARACTER SET utf8;')
			cursor.execute('SET character_set_connection=utf8;')

			return db, cursor
		else:
			return false
