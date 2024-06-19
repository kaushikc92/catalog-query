class CamelCaseTokenizer:
    """"Returns tokens separated by special delimiters (currently, capitalized letters and symbols)"""

    def __init__(self, return_set=False):
        """
        :param return_set: A flag to indicate whether to return tokens in a set instead of a list (default: False)
        """
        self.return_set = return_set # NOT USED

    def get_return_set(self):
        """
        Gets the value of the return_set flag.
        :return: The boolean value of the return_set flag.
        """
        return self.return_set

    def set_return_set(self, return_set):
        """
        Sets the value of the return_set flag.
        :param return_set: A flag to indicate whether to return tokens in a set instead of a list (default: False)
        :return:
        """
        self.return_set = return_set

    def set_type(self, c):
        if c.isalpha():
            curr_type = "letter"
        elif c.isdigit():
            curr_type = "number"
        else:
            curr_type = "symbol"
        return curr_type

    def tokenize(self, input_string):
        """
        Tokenizes input string into camel case tokens
        :param input_string:  The string to be tokenized.
        :return: A Python list of tokens if the flag return_set is False, a set of tokens otherwise.
        """
        curr_token = ''
        curr_type = None
        token_list = []

        for c in input_string:
            if len(curr_token) == 0:
                curr_token = c
                curr_type = self.set_type(c)
            else:
                if curr_type is None:
                    print("curr_type should not be None!")
                else:
                    if c.isalpha():
                        if curr_type == "letter":
                            if c.isupper():
                                if curr_token[-1].isupper():
                                    curr_token = ''.join([curr_token, c])
                                else:
                                    token_list.append(curr_token)
                                    curr_type = self.set_type(c)
                                    curr_token = c
                            else:
                                if len(curr_token) == 1:
                                    curr_token = ''.join([curr_token, c])
                                else:
                                    if curr_token[-1].isupper():
                                        token_list.append(curr_token[:-1])
                                        curr_type = self.set_type(c)
                                        curr_token = ''.join([curr_token[-1], c])
                                    else:
                                        curr_token = ''.join([curr_token, c])
                        else:
                            if curr_type != "symbol" or (curr_type == "symbol" and len(curr_token) > 1):
                                if curr_type == "number":
                                    token_list.append(str(int(curr_token)))
                                else:
                                    token_list.append(curr_token)

                            curr_type = self.set_type(c)
                            curr_token = c
                    elif c.isdigit():
                        if curr_type == "number":
                            curr_token = ''.join([curr_token, c])
                        else:
                            if curr_type != "symbol" or (curr_type == "symbol" and len(curr_token) > 1):
                                token_list.append(curr_token)

                            curr_type = self.set_type(c)
                            curr_token = c
                    else:
                        if curr_type == "symbol":
                            if curr_token[0] == c:
                                curr_token = ''.join([curr_token, c])
                            else:
                                if len(curr_token) > 1:
                                    token_list.append(curr_token)

                                curr_type = self.set_type(c)
                                curr_token = c
                        else:
                            if curr_type == "number":
                                token_list.append(str(int(curr_token)))
                            else:
                                token_list.append(curr_token)
                            curr_type = self.set_type(c)
                            curr_token = c

        if len(curr_token) != 0:
            if curr_type == "number":
                token_list.append(str(int(curr_token)))
            else:
                token_list.append(curr_token)

        return token_list

