class Connector(Database):
    def __init__(self,type):
        #describe all types in this dict
        types = {"sqlite":"?",
                 "postgre":"%s"}

        #Database.__init__(self) as necessary
        self.sql_search = "SELECT ID FROM TAG WHERE DATA = %s" % types[type]

    def _get_tag(self, tagcipher):
        #replace the sql search string with the wildcard.
        self._cur.execute(self.sql_search, ([tagcipher]))
        rv = self._cur.fetchone()
        return rv