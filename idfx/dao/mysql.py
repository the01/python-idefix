# -*- coding: UTF-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

__author__ = "d01"
__copyright__ = "Copyright (C) 2015-17, Florian JUNG"
__license__ = "All rights reserved"
__version__ = "0.2.1"
__date__ = "2017-12-07"
# Created: 2015-03-13 12:34

import datetime
import uuid

import pymysql
from flotils import Loadable, StartStopable, StartException

from ..model import Manga, User
from ..errors import DAOException, AlreadyExistsException, ValueException


class SqlConnector(Loadable, StartStopable):
    """ Connect to mysql database """

    def __init__(self, settings=None):
        if settings is None:
            settings = {}
        super(SqlConnector, self).__init__(settings)
        self._server = settings['server']
        self._default_db = settings['database']
        self._user = settings['user']
        self._pw = settings['password']
        self.connection = None
        """ Current connection
            :type : pymysql.Connection """
        self.cursor = None
        """ Current cursor
            :type : None | pymysql.cursors.Cursor """
        self._dbs = {}
        """ :type : dict[str | unicode, (pymysql.Connection, pymysql.cursors.Cursor)] """
        self.database = None
        """ Current database
            :type : None | str | unicode """

    def _connect(self, db=None, encoding="utf8"):
        """
        Connect to database

        :param db: Database to connect to
        :type db: str | unicode
        :param encoding: Database encoding
        :type db: str | unicode
        :rtype: None
        :raises DAOException: Failed to connect
        """
        if not db:
            db = self._default_db
        try:
            con = pymysql.connect(
                host=self._server,
                user=self._user,
                passwd=self._pw,
                db=db,
                charset=encoding
            )
            cur = con.cursor()
            cur.execute("SET time_zone= '+00:00'")
        except Exception as e:
            self.exception("Failed to connect")
            raise DAOException(e)
        self._dbs[db] = (con, cur)
        self.database = db
        self.connection = con
        self.cursor = cur

    def _close(self, db=None):
        """
        Commit and close database connection

        :param db: Database to connect to (default: None)
                    None means close all
        :type db: None | str | unicode
        :rtype: None
        """
        if db:
            dbs = [db]
        else:
            dbs = self._dbs.keys()

        for db in dbs:
            con, cur = self._dbs[db]

            try:
                if cur:
                    cur.close()
                if con:
                    con.commit()
                    con.close()
            except:
                self.exception("Failed to close")
            finally:
                del self._dbs[db]

                if db == self.database:
                    self.connection = None
                    self.cursor = None
                    self.database = None

    def switch_database(self, db):
        """
        Switch to database

        :param db: Database to switch to
        :type db: str | unicode
        :rtype: None
        :raises DAOException: Failed to connect
        """
        if db is None:
            db = self._default_db
        if db == self.database:
            return

        if db in self._dbs:
            # Update old values
            self._dbs[self.database] = (self.connection, self.cursor)
            # Get current
            self.connection, self.cursor = self._dbs[db]
            self.database = db
            return
        else:
            self._connect(db)

    def execute(self, cmd, args=None, db=None):
        """
        Execute a sql command

        :param cmd: Sql command to execute
        :type cmd: str | unicode
        :param args: Arguments to add (default: None)
        :type args: None | tuple[str | unicode]
        :param db: Database to execute on (default: None)
                    None means on current
        :type db:  str | unicode | None
        :return: result of .execute()
        :raises DAOException: Failed to connect
        """
        if db is not None and db != self.database:
            self.switch_database(db)

        try:
            if args:
                return self.cursor.execute(cmd, args)
            else:
                return self.cursor.execute(cmd)
        except Exception as e:
            raise DAOException(e)

    def fetchall(self, db=None):
        """
        Get all rows for database

        :param db: Database to get from (default: None)
                    None means current
        :type db: None | str | unicode
        :return: Result of .fetchall()
        :rtype: collections.iterable
        :raises DAOException: Failed to connect
        """
        if db is not None and db != self.database:
            self.switch_database(db)

        try:
            return self.cursor.fetchall()
        except Exception as e:
            raise DAOException(e)

    def commit(self, db=None):
        if db is not None and db != self.database:
            self.switch_database(db)

        try:
            return self.connection.commit()
        except Exception as e:
            raise DAOException(e)

    def rollback(self, db=None):
        if db is not None and db != self.database:
            self.switch_database(db)

        try:
            return self.connection.rollback()
        except Exception as e:
            raise DAOException(e)

    def change_encoding(self, charset, db=None):
        self.execute(
            """ALTER DATABASE python CHARACTER SET '%s'""",
            args=charset,
            db=db
        )
        return self.fetchall()

    def get_encodings(self, db=None):
        self.execute("""SHOW variables LIKE '%character_set%'""", db=db)
        return self.fetchall()

    def setup(self):
        if not self.connection:
            self._connect()

        query = "CREATE FUNCTION IF NOT EXISTS UuidToBin(_uuid BINARY(36))" \
                "  RETURNS BINARY(16)" \
                "  LANGUAGE SQL  DETERMINISTIC  CONTAINS SQL  " \
                "SQL SECURITY INVOKER" \
                " RETURN" \
                "  UNHEX(CONCAT(" \
                "    SUBSTR(_uuid, 15, 4)," \
                "    SUBSTR(_uuid, 10, 4)," \
                "    SUBSTR(_uuid,  1, 8)," \
                "    SUBSTR(_uuid, 20, 4)," \
                "    SUBSTR(_uuid, 25)" \
                "  ));" \
                "\n\n" \
                "CREATE FUNCTION IF NOT EXISTS UuidFromBin(_bin BINARY(16))" \
                "  RETURNS BINARY(36)" \
                "  LANGUAGE SQL  DETERMINISTIC  CONTAINS SQL  " \
                "SQL SECURITY INVOKER" \
                " RETURN" \
                "  LCASE(CONCAT_WS('-'," \
                "      HEX(SUBSTR(_bin,  5, 4))," \
                "      HEX(SUBSTR(_bin,  3, 2))," \
                "      HEX(SUBSTR(_bin,  1, 2))," \
                "      HEX(SUBSTR(_bin,  9, 2))," \
                "      HEX(SUBSTR(_bin, 11))" \
                "  ));"

        try:
            res = self.execute(query)
            if res:
                self.info("Created functions")
        except:
            self.exception("Failed to create functions")
            raise DAOException('Functions failed')

        query = "CREATE TABLE IF NOT EXISTS mangas (" \
                " uuid BINARY(16) NOT NULL UNIQUE PRIMARY KEY," \
                " created DATETIME NOT NULL," \
                " updated DATETIME NOT NULL," \
                " name VARCHAR(255) NOT NULL UNIQUE," \
                " latest_chapter DECIMAL(8,2) " \
                ") DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;"

        try:
            res = self.execute(query)
            if res:
                self.info("Created table mangas")
        except:
            self.exception("Failed to create table mangas")
            raise DAOException('Table create failed')

        query = "CREATE TABLE IF NOT EXISTS users (" \
                " uuid BINARY(16) NOT NULL UNIQUE PRIMARY KEY," \
                " created DATETIME NOT NULL," \
                " updated DATETIME NOT NULL," \
                " firstname VARCHAR(255) NOT NULL," \
                " lastname VARCHAR(255) NOT NULL," \
                " role INTEGER NOT NULL," \
                " CONSTRAINT users_name_uq UNIQUE (lastname, firstname)" \
                ") DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;"

        try:
            res = self.execute(query)
            if res:
                self.info("Created table users")
        except:
            self.exception("Failed to create table users")
            raise DAOException('Table create failed')

        query = "CREATE TABLE IF NOT EXISTS mangas_read (" \
            " user_uuid BINARY(16) NOT NULL," \
            " manga_uuid BINARY(16) NOT NULL," \
            " created DATETIME NOT NULL," \
            " updated DATETIME NOT NULL," \
            " chapter DECIMAL(8,2)," \
            " CONSTRAINT mangas_read_pk PRIMARY KEY (user_uuid, manga_uuid)," \
            " CONSTRAINT mangas_read_user_fk FOREIGN KEY (user_uuid)" \
            "   REFERENCES users (uuid) ON DELETE CASCADE," \
            " CONSTRAINT mangas_read_manga_fk FOREIGN KEY (manga_uuid)" \
            "   REFERENCES mangas (uuid) ON DELETE CASCADE," \
            " CONSTRAINT mangas_read_pk_uq UNIQUE (user_uuid, manga_uuid)" \
            ") DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;"

        try:
            res = self.execute(query)
            if res:
                self.info("Created table mangas_read")
        except:
            self.exception("Failed to create table mangas_read")
            raise DAOException('Table create failed')

    def manga_create(self, manga):
        """
        New manga

        :param manga: Manga to create
        :type manga: idefix.model.Manga
        :return: Affected entries
        :rtype: int
        :raises DAOException: Failure
        """
        if not manga:
            raise ValueException("Invalid manga")
        if not manga.created:
            manga.created = datetime.datetime.utcnow()
        if not manga.updated:
            manga.updated = manga.created
        if not manga.uuid:
            manga.uuid = "{}".format(uuid.uuid4())
        try:
            affected = self.execute(
                "INSERT INTO mangas ("
                "  uuid,created,updated,name,latest_chapter"
                ") VALUES "
                "(UuidToBin(%s),%s,%s,%s,%s);",
                (
                    manga.uuid, manga.created, manga.updated,
                    manga.name, manga.latest_chapter
                )
            )
            # self.debug(affected)
            return affected
        except DAOException as e:
            if e.args[0] and e.args[0].args and e.args[0].args[0] == 1062:
                # Double entry
                raise AlreadyExistsException(e.args[0].args[1])
            raise
        except Exception as e:
            raise DAOException(e)

    def manga_update(self, manga):
        """
        Update manga

        :param manga: Manga to update
        :type manga: idefix.model.Manga
        :return: Affected entries
        :rtype: int
        :raises DAOException: Failure
        """
        if not manga or not manga.uuid:
            raise ValueException("Invalid manga")
        if not manga.updated:
            manga.updated = datetime.datetime.utcnow()
        query = "UPDATE mangas SET updated=%s"
        args = (manga.updated,)
        if manga.name is not None:
            query += ",name=%s"
            args += (manga.name,)
        if manga.latest_chapter is not None:
            query += ",latest_chapter=%s"
            args += (manga.latest_chapter,)
        try:
            affected = self.execute(
                query + " WHERE UuidFromBin(uuid)=%s",
                args + (manga.uuid,)
            )
            # self.debug(affected)
            return affected
        except DAOException as e:
            if e.args[0] and e.args[0].args and e.args[0].args[0] == 1062:
                # Double entry
                raise AlreadyExistsException(e.args[0].args[1])
            raise
        except Exception as e:
            raise DAOException(e)

    def manga_delete(self, manga):
        """
        Delete manga

        :param manga: Manga to delete
        :type manga: idefix.model.Manga
        :return: Affected entries
        :rtype: int
        :raises DAOException: Failure
        """
        if not manga or not manga.uuid:
            raise ValueException("Invalid manga")
        try:
            affected = self.execute(
                "DELETE FROM mangas WHERE UuidFromBin(uuid)=%s",
                (manga.uuid,)
            )
            # self.debug(affected)
            return affected
        except DAOException:
            raise
        except Exception as e:
            raise DAOException(e)

    def manga_get(self, manga=None, use_like=False):
        """
        Get manga

        :param manga: Manga to get or None for all (default: None)
        :type manga: None | idefix.model.Manga
        :param use_like: Use LIKE for comparison (default: False)
        :type use_like: bool
        :return: Found mangas
        :rtype: list[idefix.model.Manga]
        :raises DAOException: Failure
        """
        args = ()
        where_query = ""
        where_query_parts = []
        if manga:
            if manga.name:
                op = "LIKE" if use_like else "="
                where_query_parts.append(("name", op, manga.name))
            if manga.uuid:
                # Unique, other values not necessary
                where_query_parts = [("UuidFromBin(uuid)", "=", manga.uuid)]

        if where_query_parts:
            where_query = " WHERE " + " AND ".join(
                ["{} {} %s".format(t[0], t[1]) for t in where_query_parts]
            )
            args = tuple([t[2] for t in where_query_parts])
        mangas = []
        try:
            affected = self.execute(
                "SELECT"
                " UuidFromBin(uuid),created,updated,name,latest_chapter "
                "FROM mangas" + where_query
                , args)
            # self.debug(affected)
            if affected:
                res = self.fetchall()
                for t in res:
                    mangas.append(Manga(
                        uuid=t[0], name=t[3]
                    ))
                    mangas[-1].created = t[1]
                    mangas[-1].updated = t[2]
                    mangas[-1].latest_chapter = float(t[4])
        except DAOException:
            raise
        except Exception as e:
            raise DAOException(e)
        return mangas

    def user_create(self, user):
        """
        New user

        :param user: User to create
        :type user: idefix.model.User
        :return: Affected entries
        :rtype: int
        :raises DAOException: Failure
        """
        if not user:
            raise ValueException("Invalid user")
        if not user.created:
            user.created = datetime.datetime.utcnow()
        if not user.updated:
            user.updated = user.created
        if not user.uuid:
            user.uuid = "{}".format(uuid.uuid4())
        try:
            affected = self.execute(
                "INSERT INTO users ("
                "  uuid,created,updated,firstname,lastname,role"
                ") VALUES "
                "(UuidToBin(%s),%s,%s,%s,%s,%s);",
                (
                    user.uuid, user.created, user.updated,
                    user.firstname, user.lastname, user.role
                )
            )
            # self.debug(affected)
            return affected
        except DAOException as e:
            if e.args[0] and e.args[0].args and e.args[0].args[0] == 1062:
                # Double entry
                raise AlreadyExistsException(e.args[0].args[1])
            raise
        except Exception as e:
            raise DAOException(e)

    def user_update(self, user):
        """
        Update user

        :param user: User to update
        :type user: idefix.model.User
        :return: Affected entries
        :rtype: int
        :raises DAOException: Failure
        """
        if not user or not user.uuid:
            raise ValueException("Invalid user")
        if not user.updated:
            user.updated = datetime.datetime.utcnow()
        query = "UPDATE users SET updated=%s"
        args = (user.updated,)
        if user.firstname is not None:
            query += ",firstname=%s"
            args += (user.firstname,)
        if user.lastname is not None:
            query += ",lastname=%s"
            args += (user.lastname,)
        try:
            affected = self.execute(
                query + " WHERE UuidFromBin(uuid)=%s",
                args + (user.uuid,)
            )
            # self.debug(affected)
            return affected
        except DAOException as e:
            if e.args[0] and e.args[0].args and e.args[0].args[0] == 1062:
                # Double entry
                raise AlreadyExistsException(e.args[0].args[1])
            raise
        except Exception as e:
            raise DAOException(e)

    def user_delete(self, user):
        """
        Delete user

        :param user: User to delete
        :type user: idefix.model.User
        :return: Affected entries
        :rtype: int
        :raises DAOException: Failure
        """
        if not user or not user.uuid:
            raise ValueException("Invalid user")
        try:
            affected = self.execute(
                "DELETE FROM users WHERE UuidFromBin(uuid)=%s",
                (user.uuid,)
            )
            # self.debug(affected)
            return affected
        except DAOException:
            raise
        except Exception as e:
            raise DAOException(e)

    def user_get(self, user=None, use_like=False):
        """
        Find user

        :param user: User to get or None for all (default: None)
        :type user: None | idefix.model.User
        :param use_like: Use LIKE for comparison (default: False)
        :type use_like: bool
        :return: Found users
        :rtype: list[idefix.model.User]
        :raises DAOException: Failure
        """
        args = ()
        where_query = ""
        where_query_parts = []
        if user:
            if user.firstname:
                op = "LIKE" if use_like else "="
                where_query_parts.append(
                    ("firstname", op, user.firstname.lower())
                )
            if user.lastname:
                op = "LIKE" if use_like else "="
                where_query_parts.append(
                    ("lastname", op, user.lastname.lower())
                )
            if user.uuid:
                # Unique, other values not necessary
                where_query_parts = [("UuidFromBin(uuid)", "=", user.uuid)]

        if where_query_parts:
            where_query = " WHERE " + " AND ".join(
                ["{} {} %s".format(t[0], t[1]) for t in where_query_parts]
            )
            args = tuple([t[2] for t in where_query_parts])
        users = []
        try:
            affected = self.execute(
                "SELECT"
                " UuidFromBin(uuid),created,updated,firstname,lastname,role "
                "FROM users" + where_query
                , args)
            # self.debug(affected)
            if affected:
                res = self.fetchall()
                for t in res:
                    users.append(User(
                        uuid=t[0], firstname=t[3], lastname=t[4]
                    ))
                    users[-1].created = t[1]
                    users[-1].updated = t[2]
                    users[-1].role = t[5]
        except DAOException:
            raise
        except Exception as e:
            raise DAOException(e)
        return users

    def read_create(self, user, manga):
        """
        New user read manga

        :param user: User to create
        :type user: idefix.model.User
        :param manga: Manga to create
        :type manga: idefix.model.Manga
        :return: Affected entries
        :rtype: int
        :raises DAOException: Failure
        """
        if not user or not user.uuid:
            raise ValueException("Invalid user")
        if not manga or not manga.uuid:
            raise ValueException("Invalid manga")
        chapter = 0
        if manga.chapter:
            chapter = manga.chapter
        now = datetime.datetime.utcnow()
        if not manga.created:
            manga.created = now
        if not manga.updated:
            manga.updated = now
        try:
            affected = self.execute(
                "INSERT INTO mangas_read ("
                "  user_uuid,manga_uuid,created,updated,chapter"
                ") VALUES "
                "(UuidToBin(%s),UuidToBin(%s),%s,%s,%s);",
                (user.uuid, manga.uuid, manga.created, manga.updated, chapter)
            )
            # self.debug(affected)
            return affected
        except DAOException as e:
            if e.args[0] and e.args[0].args and e.args[0].args[0] == 1062:
                # Double entry
                raise AlreadyExistsException(e.args[0].args[1])
            raise
        except Exception as e:
            raise DAOException(e)

    def read_update(self, user, manga):
        """
        Update user read manga

        :param user: User to update
        :type user: idefix.model.User
        :param manga: Manga to update
        :type manga: idefix.model.Manga
        :return: Affected entries
        :rtype: int
        :raises DAOException: Failure
        """
        if not user or not user.uuid:
            raise ValueException("Invalid user")
        if not manga or not manga.uuid or manga.chapter is None:
            raise ValueException("Invalid manga")
        now = datetime.datetime.utcnow()
        try:
            affected = self.execute(
                "UPDATE mangas_read SET updated=%s,chapter=%s"
                " WHERE "
                "UuidFromBin(user_uuid)=%s AND UuidFromBin(manga_uuid)=%s",
                (now, manga.chapter, user.uuid, manga.uuid)
            )
            # self.debug(affected)
            return affected
        except DAOException as e:
            if e.args[0] and e.args[0].args and e.args[0].args[0] == 1062:
                # Double entry
                raise AlreadyExistsException(e.args[0].args[1])
            raise
        except Exception as e:
            raise DAOException(e)

    def read_delete(self, user, manga):
        """
        Delete user read manga

        :param user: User to delete
        :type user: idefix.model.User
        :param manga: Manga to delete
        :type manga: idefix.model.Manga
        :return: Affected entries
        :rtype: int
        :raises DAOException: Failure
        """
        if not user or not user.uuid:
            raise ValueException("Invalid user")
        if not manga or not manga.uuid:
            raise ValueException("Invalid manga")
        try:
            affected = self.execute(
                "DELETE FROM mangas_read"
                " WHERE "
                "UuidFromBin(user_uuid)=%s AND UuidFromBin(manga_uuid)=%s",
                (user.uuid, manga.uuid)
            )
            # self.debug(affected)
            return affected
        except DAOException:
            raise
        except Exception as e:
            raise DAOException(e)

    def read_get(self, user, manga=None, use_like=False):
        """
        Find user read manga

        :param user: User to find for
        :type user: idefix.model.User
        :param manga: Manga being read to get or None for all (default: None)
        :type manga: None | idefix.model.Manga
        :param use_like: Use LIKE for comparison (default: False)
        :type use_like: bool
        :return: Found mangas
        :rtype: list[idefix.model.Manga]
        :raises DAOException: Failure
        """
        if not user or not user.uuid:
            raise ValueException("Invalid user")
        args = (user.uuid,)
        where_query = " WHERE UuidFromBin(user_uuid)=%s"
        where_query_parts = []
        if manga:
            if manga.name:
                op = "LIKE" if use_like else "="
                where_query_parts.append(("m.name", op, manga.name))
            if manga.chapter:
                where_query_parts.append(("mr.chapter", "=", manga.chapter))
            if manga.uuid:
                # Unique, other values not necessary
                where_query_parts = [(
                    "UuidFromBin(mr.manga_uuid)", "=", manga.uuid
                )]

        if where_query_parts:
            where_query += " AND " + " AND ".join(
                ["{} {} %s".format(t[0], t[1]) for t in where_query_parts]
            )
            args += tuple([t[2] for t in where_query_parts])
        mangas = []
        try:
            affected = self.execute(
                "SELECT"
                " UuidFromBin(m.uuid),m.name,m.latest_chapter,"
                "mr.created,mr.updated,mr.chapter "
                "FROM mangas_read mr JOIN mangas m ON mr.manga_uuid = m.uuid"
                + where_query
                , args)
            # self.debug(affected)
            if affected:
                res = self.fetchall()
                for t in res:
                    mangas.append(Manga(
                        uuid=t[0], name=t[1], chapter=float(t[5])
                    ))
                    mangas[-1].created = t[3]
                    mangas[-1].updated = t[4]
                    mangas[-1].latest_chapter = float(t[2])
        except DAOException:
            raise
        except Exception as e:
            raise DAOException(e)
        return mangas

    def read_get_index(self):
        """
        Get mangas linked to users reading them

        :return: Manga - user/chapter correlation
        :rtype: list[(idefix.model.Manga, list[(str | unicode, int)]]
        """
        mangas = []
        try:
            affected = self.execute(
                "SELECT"
                " UuidFromBin(m.uuid),m.name,"
                " mr.chapter,UuidFromBin(mr.user_uuid) "
                "FROM mangas_read mr JOIN mangas m ON mr.manga_uuid = m.uuid "
                "ORDER BY m.uuid"
            )
            if affected:
                res = self.fetchall()
                for t in res:
                    if not mangas or mangas[-1][0].uuid != t[0]:
                        mangas.append((
                            Manga(uuid=t[0], name=t[1]), []
                        ))
                    mangas[-1][1].append((t[3], float(t[2])))
        except DAOException:
            raise
        except Exception as e:
            raise DAOException(e)
        # sanity check
        mids = [m.uuid for m, _ in mangas]
        smids = set(mids)
        if len(mids) != len(smids):
            self.error("Got double entries {}-{}".format(len(mids), len(smids)))
        return mangas

    def start(self, blocking=False):
        self.debug("()")
        try:
            self._connect()
        except:
            # self.exception("Failed to connect")
            raise StartException("Connecting failed")
        super(SqlConnector, self).start(blocking)

    def stop(self):
        self.debug("()")
        try:
            self._close()
        except:
            self.exception("Failed to close")
        super(SqlConnector, self).stop()
