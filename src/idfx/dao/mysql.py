# -*- coding: UTF-8 -*-
""" DAO implementation for mysql (mariadb) """

__author__ = "d01"
__copyright__ = "Copyright (C) 2015-23, Florian JUNG"
__license__ = "All rights reserved"
__version__ = "0.3.1"
__date__ = "2023-06-13"
# Created: 2015-03-13 12:34

import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
import uuid

from flotils import Loadable, StartException, StartStopable
import pymysql.cursors

from ..errors import AlreadyExistsException, DAOException, ValueException
from ..model import Manga, User


class SqlConnector(Loadable, StartStopable):
    """ Connect to mysql database """

    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        """ Constructor """
        if settings is None:
            settings = {}

        super().__init__(settings)
        self._server: str = settings['server']
        """ How to connect to server """
        self._default_db: str = settings['database']
        """ Default db to use """
        self._user: str = settings['user']
        """ DB user """
        self._pw: str = settings['password']
        """ DB password """
        self.connection: Optional[pymysql.Connection] = None
        """ Current connection """
        self.cursor: Optional[pymysql.cursors.Cursor] = None
        """ Current cursor """
        self._dbs: Dict[
            str, Tuple[pymysql.Connection, pymysql.cursors.Cursor]
        ] = {}
        """ Connections for different databases """
        self.database: Optional[str] = None
        """ Current database """

    def _connect(self, db: Optional[str] = None, encoding: str = "utf8") -> None:
        """
        Connect to database

        :param db: Database to connect to
        :param encoding: Database encoding
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

    def _close(self, db: Optional[str] = None) -> None:
        """
        Commit and close database connection

        :param db: Database to connect to (default: None)
                    None means close all
        """
        if db:
            dbs = [db]
        else:
            dbs = list(self._dbs.keys())

        for db in dbs:
            con, cur = self._dbs[db]

            try:
                if cur:
                    cur.close()

                if con:
                    con.commit()
                    con.close()
            except Exception:
                self.exception("Failed to close")
            finally:
                del self._dbs[db]

                if db == self.database:
                    self.connection = None
                    self.cursor = None
                    self.database = None

    def switch_database(self, db: str) -> None:
        """
        Switch to database

        :param db: Database to switch to
        :raises DAOException: Failed to connect
        """
        if db is None:
            db = self._default_db
        if db == self.database:
            return
        if self.database is None:
            raise DAOException("No database set")

        if db in self._dbs:
            # Update old values
            if self.connection is None or self.cursor is None:
                raise DAOException("No connection or cursor set")

            self._dbs[self.database] = (self.connection, self.cursor)
            # Get current
            self.connection, self.cursor = self._dbs[db]
            self.database = db
        else:
            self._connect(db)

    def execute(
            self,
            cmd: str,
            args: Optional[Tuple] = None,
            db: Optional[str] = None,
    ) -> int:
        """
        Execute a sql command

        :param cmd: Sql command to execute
        :param args: Arguments to add (default: None)
        :param db: Database to execute on (default: None)
                    None means on current
        :return: result of .execute() (number of affected rows)
        :raises DAOException: Failed to connect
        """
        if db is not None and db != self.database:
            self.switch_database(db)

        if not self.cursor:
            raise DAOException("No cursor")

        try:
            if args:
                return self.cursor.execute(cmd, args)
            else:
                return self.cursor.execute(cmd)
        except Exception as e:
            raise DAOException(e)

    def fetchall(self, db: Optional[str] = None) -> Iterable[Any]:
        """
        Get all rows for database

        :param db: Database to get from (default: None)
                    None means current
        :return: Result of .fetchall()
        :raises DAOException: Failed to connect
        """
        if db is not None and db != self.database:
            self.switch_database(db)

        if not self.cursor:
            raise DAOException("No cursor")

        try:
            return self.cursor.fetchall()
        except Exception as e:
            raise DAOException(e)

    def commit(self, db: Optional[str] = None) -> None:
        """
        Commit current transaction

        :param db: Database to commit (default: None)
                    None means current
        :raises DAOException: Failed to connect
        """
        if db is not None and db != self.database:
            self.switch_database(db)

        if not self.connection:
            raise DAOException("No connection")

        try:
            return self.connection.commit()
        except Exception as e:
            raise DAOException(e)

    def rollback(self, db: Optional[str] = None) -> None:
        """
        Rollback current transaction

        :param db: Database to rollback (default: None)
                    None means current
        :raises DAOException: Failed to connect
        """
        if db is not None and db != self.database:
            self.switch_database(db)

        if not self.connection:
            raise DAOException("No connection")

        try:
            return self.connection.rollback()
        except Exception as e:
            raise DAOException(e)

    def change_encoding(self, charset: str, db: Optional[str] = None) -> Iterable[Any]:
        """
        Change charset of database

        :param charset: Charset to use for db
        :param db: Database to get from (default: None)
                    None means currentNone) -> None:
        :return: Result from fetchall after change
        :raises DAOException: Failed to connect
        """
        self.execute(
            """ALTER DATABASE python CHARACTER SET '%s'""",
            # TODO: Check tuple needed?
            args=(charset, ),
            db=db
        )

        return self.fetchall()

    def get_encodings(self, db: Optional[str] = None) -> Iterable[Any]:
        """
        Get current charset of database

        :param db: Database to get from (default: None)
                    None means currentNone) -> None:
        :return: Result from fetchall
        :raises DAOException: Failed to connect
        """
        self.execute("""SHOW variables LIKE '%character_set%'""", db=db)

        return self.fetchall()

    def setup(self) -> None:
        """
        Prepare db for run (setup connnection, tables, ..)

        :raises DAOException: Failed to connect
        """
        if not self.connection:
            self._connect()

        # Transform between binary uuid and str(byte)
        query = \
            "CREATE FUNCTION IF NOT EXISTS UuidToBin(_uuid BINARY(36))" \
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
        except Exception:
            self.exception("Failed to create functions")

            raise DAOException("Functions failed")

        if res:
            self.info("Created functions")

        query = \
            "CREATE TABLE IF NOT EXISTS mangas (" \
            " uuid BINARY(16) NOT NULL UNIQUE PRIMARY KEY," \
            " created DATETIME NOT NULL," \
            " updated DATETIME NOT NULL," \
            " name VARCHAR(255) NOT NULL UNIQUE," \
            " latest_chapter DECIMAL(8,2) " \
            ") DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;"

        try:
            res = self.execute(query)
        except Exception:
            self.exception("Failed to create table mangas")

            raise DAOException("Table create failed")

        if res:
            self.info("Created table mangas")

        query = \
            "CREATE TABLE IF NOT EXISTS users (" \
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
        except Exception:
            self.exception("Failed to create table users")

            raise DAOException("Table create failed")

        if res:
            self.info("Created table users")

        query = \
            "CREATE TABLE IF NOT EXISTS mangas_read (" \
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
        except Exception:
            self.exception("Failed to create table mangas_read")

            raise DAOException("Table create failed")

        if res:
            self.info("Created table mangas_read")

    def manga_create(self, manga: Manga) -> int:
        """
        New manga

        :param manga: Manga to create
        :return: Affected entries
        :raises DAOException: Failure
        """
        if not manga:
            raise ValueException("Invalid manga")
        if not manga.created:
            manga.created = datetime.datetime.utcnow()
        if not manga.updated:
            manga.updated = manga.created
        if not manga.uuid:
            manga.uuid = f"{uuid.uuid4()}"

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

            return affected
        except DAOException as e:
            if e.args[0] and e.args[0].args and e.args[0].args[0] == 1062:
                # Double entry
                raise AlreadyExistsException(e.args[0].args[1])

            raise
        except Exception as e:
            raise DAOException(e)

    def manga_update(self, manga: Manga) -> int:
        """
        Update manga

        :param manga: Manga to update
        :return: Affected entries
        :raises DAOException: Failure
        """
        if not manga or not manga.uuid:
            raise ValueException("Invalid manga")

        if not manga.updated:
            manga.updated = datetime.datetime.utcnow()

        query = "UPDATE mangas SET updated=%s"
        args: Tuple[Union[datetime.datetime, str, float], ...] = (manga.updated,)

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

            return affected
        except DAOException as e:
            if e.args[0] and e.args[0].args and e.args[0].args[0] == 1062:
                # Double entry
                raise AlreadyExistsException(e.args[0].args[1])

            raise
        except Exception as e:
            raise DAOException(e)

    def manga_delete(self, manga: Manga) -> int:
        """
        Delete manga

        :param manga: Manga to delete
        :return: Affected entries
        :raises DAOException: Failure
        """
        if not manga or not manga.uuid:
            raise ValueException("Invalid manga")

        try:
            affected = self.execute(
                "DELETE FROM mangas WHERE UuidFromBin(uuid)=%s",
                (manga.uuid,)
            )

            return affected
        except DAOException:
            raise
        except Exception as e:
            raise DAOException(e)

    def manga_get(
            self, manga: Optional[Manga] = None, use_like: bool = False
    ) -> List[Manga]:
        """
        Get manga

        :param manga: Manga to get or None for all (default: None)
        :param use_like: Use LIKE for comparison (default: False)
        :return: Found mangas
        :raises DAOException: Failure
        """
        args: Tuple[str, ...] = ()
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
            where_query = " WHERE " + " AND ".join([
                f"{t[0]} {t[1]} %s"
                for t in where_query_parts
            ])
            args = tuple([
                t[2]
                for t in where_query_parts
            ])

        mangas = []

        try:
            affected = self.execute(
                "SELECT"
                " UuidFromBin(uuid),created,updated,name,latest_chapter "
                "FROM mangas" + where_query,
                args
            )

            if affected:
                res = self.fetchall()

                for t in res:
                    mangas.append(Manga(
                        # Uuid is binary in db
                        uuid=t[0].decode("utf-8"), name=t[3]
                    ))
                    mangas[-1].created = t[1]
                    mangas[-1].updated = t[2]
                    mangas[-1].latest_chapter = float(t[4])
        except DAOException:
            raise
        except Exception as e:
            raise DAOException(e)

        return mangas

    def user_create(self, user: User) -> int:
        """
        New user

        :param user: User to create
        :return: Affected entries
        :raises DAOException: Failure
        """
        if not user:
            raise ValueException("Invalid user")

        if not user.created:
            user.created = datetime.datetime.utcnow()
        if not user.updated:
            user.updated = user.created
        if not user.uuid:
            user.uuid = f"{uuid.uuid4()}"

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

            return affected
        except DAOException as e:
            if e.args[0] and e.args[0].args and e.args[0].args[0] == 1062:
                # Double entry
                raise AlreadyExistsException(e.args[0].args[1])

            raise
        except Exception as e:
            raise DAOException(e)

    def user_update(self, user: User) -> int:
        """
        Update user

        :param user: User to update
        :return: Affected entries
        :raises DAOException: Failure
        """
        if not user or not user.uuid:
            raise ValueException("Invalid user")

        if not user.updated:
            user.updated = datetime.datetime.utcnow()

        query = "UPDATE users SET updated=%s"
        args: Tuple[Union[datetime.datetime, str], ...] = (user.updated,)

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

            return affected
        except DAOException as e:
            if e.args[0] and e.args[0].args and e.args[0].args[0] == 1062:
                # Double entry
                raise AlreadyExistsException(e.args[0].args[1])

            raise
        except Exception as e:
            raise DAOException(e)

    def user_delete(self, user: User) -> int:
        """
        Delete user

        :param user: User to delete
        :return: Affected entries
        :raises DAOException: Failure
        """
        if not user or not user.uuid:
            raise ValueException("Invalid user")

        try:
            affected = self.execute(
                "DELETE FROM users WHERE UuidFromBin(uuid)=%s",
                (user.uuid,)
            )

            return affected
        except DAOException:
            raise
        except Exception as e:
            raise DAOException(e)

    def user_get(
            self, user: Optional[User] = None, use_like: bool = False
    ) -> List[User]:
        """
        Find user

        :param user: User to get or None for all (default: None)
        :param use_like: Use LIKE for comparison (default: False)
        :return: Found users
        :raises DAOException: Failure
        """
        args: Tuple[Union[float, str], ...] = ()
        where_query = ""
        where_query_parts: List[Tuple[str, str, str]] = []

        if user:
            if user.firstname:
                op = "LIKE" if use_like else "="
                where_query_parts.append(
                    ("firstname", op, user.firstname)
                )
            if user.lastname:
                op = "LIKE" if use_like else "="
                where_query_parts.append(
                    ("lastname", op, user.lastname)
                )
            if user.uuid:
                # Unique, other values not necessary
                where_query_parts = [("UuidFromBin(uuid)", "=", user.uuid)]

        if where_query_parts:
            where_query = " WHERE " + " AND ".join(
                [f"{t[0]} {t[1]} %s" for t in where_query_parts]
            )
            args = tuple([
                t[2]
                for t in where_query_parts
            ])

        users = []

        try:
            affected = self.execute(
                "SELECT"
                " UuidFromBin(uuid),created,updated,firstname,lastname,role "
                "FROM users" + where_query,
                args
            )

            if affected:
                res = self.fetchall()

                for t in res:
                    users.append(User(
                        uuid=t[0].decode('utf-8'), firstname=t[3], lastname=t[4]
                    ))
                    users[-1].created = t[1]
                    users[-1].updated = t[2]
                    users[-1].role = t[5]
        except DAOException:
            raise
        except Exception as e:
            raise DAOException(e)

        return users

    def read_create(self, user: User, manga: Manga) -> int:
        """
        New user read manga

        :param user: User to create
        :param manga: Manga to create
        :return: Affected entries
        :raises DAOException: Failure
        """
        if not user or not user.uuid:
            raise ValueException("Invalid user")
        if not manga or not manga.uuid:
            raise ValueException("Invalid manga")

        chapter = 0.0

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

            return affected
        except DAOException as e:
            if e.args[0] and e.args[0].args and e.args[0].args[0] == 1062:
                # Double entry
                raise AlreadyExistsException(e.args[0].args[1])

            raise
        except Exception as e:
            raise DAOException(e)

    def read_update(self, user: User, manga: Manga) -> int:
        """
        Update user read manga

        :param user: User to update
        :param manga: Manga to update
        :return: Affected entries
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

            return affected
        except DAOException as e:
            if e.args[0] and e.args[0].args and e.args[0].args[0] == 1062:
                # Double entry
                raise AlreadyExistsException(e.args[0].args[1])

            raise
        except Exception as e:
            raise DAOException(e)

    def read_delete(self, user: User, manga: Manga) -> int:
        """
        Delete user read manga

        :param user: User to delete
        :param manga: Manga to delete
        :return: Affected entries
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

            return affected
        except DAOException:
            raise
        except Exception as e:
            raise DAOException(e)

    def read_get(
            self, user: User, manga: Optional[Manga] = None, use_like: bool = False
    ) -> List[Manga]:
        """
        Find user read manga

        :param user: User to find for
        :param manga: Manga being read to get or None for all (default: None)
        :param use_like: Use LIKE for comparison (default: False)
        :return: Found mangas
        :raises DAOException: Failure
        """
        if not user or not user.uuid:
            raise ValueException("Invalid user")

        args: Tuple[Union[float, str], ...] = (str(user.uuid),)
        where_query = " WHERE UuidFromBin(user_uuid)=%s"
        where_query_parts: List[Tuple[str, str, Union[float, str]]] = []

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
            where_query += " AND " + " AND ".join([
                f"{t[0]} {t[1]} %s"
                for t in where_query_parts
            ])
            args += tuple([
                t[2]
                for t in where_query_parts
            ])

        mangas = []

        try:
            query = \
                """
                SELECT
                    UuidFromBin(m.uuid),m.name,m.latest_chapter,
                    mr.created,mr.updated,mr.chapter
                FROM mangas_read mr JOIN mangas m ON mr.manga_uuid = m.uuid
                """
            query += where_query
            affected = self.execute(query, args)

            if affected:
                res = self.fetchall()

                for t in res:
                    mangas.append(Manga(
                        uuid=t[0].decode("utf-8"), name=t[1], chapter=float(t[5])
                    ))
                    mangas[-1].created = t[3]
                    mangas[-1].updated = t[4]
                    mangas[-1].latest_chapter = float(t[2])
        except DAOException:
            raise
        except Exception as e:
            raise DAOException(e)

        return mangas

    def read_get_index(self) -> List[Tuple[Manga, List[Tuple[str, float]]]]:
        """
        Get mangas linked to users reading them

        :return: Manga - user/chapter correlation
        """
        mangas: List[Tuple[Manga, List[Tuple[str, float]]]] = []

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
                            Manga(uuid=t[0].decode("utf-8"), name=t[1]),
                            []
                        ))

                    mangas[-1][1].append(
                        (t[3], float(t[2]))
                    )
        except DAOException:
            raise
        except Exception as e:
            raise DAOException(e)

        # sanity check
        mids = [m.uuid for m, _ in mangas]
        smids = set(mids)

        if len(mids) != len(smids):
            self.error(f"Got double entries {len(mids)}-{len(smids)}")

        return mangas

    def start(self, blocking: bool = False) -> None:
        """
        Start interface

        :param blocking: Run start until done
        """
        self.debug("()")

        try:
            self._connect()
        except Exception:
            # self.exception("Failed to connect")
            raise StartException("Connecting failed")

        super().start(blocking)

    def stop(self) -> None:
        """ Stop interface """
        self.debug("()")

        try:
            self._close()
        except Exception:
            self.exception("Failed to close")

        super().stop()
