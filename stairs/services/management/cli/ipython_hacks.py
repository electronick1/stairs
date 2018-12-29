import re

from . import const
from .namespace import CliApp


def get_python_matches(self):

    def magic_matches(text):
        """Match magics"""
        line_magics = const.MAGIC_COMMANDS
        cell_magics = const.MAGIC_COMMANDS

        pre = self.magic_escape
        pre2 = pre+pre

        explicit_magic = text.startswith(pre)

        bare_text = text.lstrip(pre)
        global_matches = self.global_matches(bare_text)
        if not explicit_magic:
            def matches(magic):
                """
                Filter magics, in particular remove magics that match
                a name present in global namespace.
                """
                return (magic.startswith(bare_text) and
                        magic not in global_matches)
        else:
            def matches(magic):
                return magic.startswith(bare_text)

        comp = [m for m in cell_magics if matches(m)]
        if not text.startswith(pre2):
            comp += [m for m in line_magics if matches(m)]

        return comp

    def global_matches(text):
        """Compute matches when text is a simple name.

        Return a list of all keywords, built-in functions and names currently
        defined in self.namespace or self.global_namespace that match.

        """
        matches = []
        match_append = matches.append
        n = len(text)
        for lst in [self.namespace.keys(),
                    self.global_namespace.keys()]:
            for word in lst:
                if word[:n] == text and word != "__builtins__":
                    match_append(word)

        snake_case_re = re.compile(r"[^_]+(_[^_]+)+?\Z")
        for lst in [self.namespace.keys(),
                    self.global_namespace.keys()]:
            shortened = {"_".join([sub[0] for sub in word.split('_')]) : word
                         for word in lst if snake_case_re.match(word)}
            for word in shortened.keys():
                if word[:n] == text and word != "__builtins__":
                    match_append(shortened[word])
        return matches

    def _callable_postfix(val, word):
        return word

    def attr_matches(text):
        """Compute matches when text contains a dot.

        Assuming the text is of the form NAME.NAME....[NAME], and is
        evaluable in self.namespace, it will be evaluated and its attributes
        (as revealed by dir()) are used as possible completions.  (For class
        instances, class members are also considered.)

        WARNING: this can still invoke arbitrary C code, if an object
        with a __getattr__ hook is evaluated.

        """
        import re
        m = re.match(r"(\w+(\.\w+)*)\.(\w*)", text)
        if not m:
            return []
        expr, attr = m.group(1, 3)
        try:
            thisobject = eval(expr, self.namespace)
        except Exception:
            return []

        # get the content of the object, except __builtins__
        words = dir(thisobject)

        if isinstance(thisobject, CliApp):
            if 'producer' in self.line_buffer:
                words = thisobject.producers

        if "__builtins__" in words:
            words.remove("__builtins__")

        matches = []
        n = len(attr)
        for word in words:
            if word[:n] == attr and hasattr(thisobject, word):
                val = getattr(thisobject, word)
                word = _callable_postfix(val, "%s.%s" % (expr, word))
                matches.append(word)
        return matches

    def python_matches(text):
        """Match attributes or global python names"""
        matches = []
        if "." in text:
            try:
                matches = attr_matches(text)
                if text.endswith('.') and self.omit__names:
                    if self.omit__names == 1:
                        # true if txt is _not_ a __ name, false otherwise:
                        no__name = (lambda txt:
                                    re.match(r'.*\.__.*?__',txt) is None)
                    else:
                        # true if txt is _not_ a _ name, false otherwise:
                        no__name = (lambda txt:
                                    re.match(r'\._.*?',txt[txt.rindex('.'):]) is None)
                    matches = filter(no__name, matches)
            except NameError:
                # catches <undefined attributes>.<tab>
                matches = []
        else:
            matches = global_matches(text)
        return matches

    return python_matches, magic_matches
