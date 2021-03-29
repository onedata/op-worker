%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% File attributes record definition.
%%% @end
%%%--------------------------------------------------------------------
-ifndef(FILE_ATTR_HRL).
-define(FILE_ATTR_HRL, 1).

%% File types
-define(REGULAR_FILE_TYPE, 'REG').
-define(DIRECTORY_TYPE, 'DIR').
-define(LINK_TYPE, 'LNK'). % hard link
-define(SYMLINK_TYPE, 'SYMLNK'). % symbolic link

-record(file_attr, {
    guid :: undefined | binary() | atom() | integer(),
    name :: binary(),
    mode :: non_neg_integer(),
    parent_guid :: undefined | binary(),
    uid = 0 :: non_neg_integer(),
    gid = 0 :: non_neg_integer(),
    atime = 0 :: non_neg_integer(),
    mtime = 0 :: non_neg_integer(),
    ctime = 0 :: non_neg_integer(),
    type :: ?REGULAR_FILE_TYPE | ?DIRECTORY_TYPE | ?LINK_TYPE | ?SYMLINK_TYPE,
    size = 0 :: undefined | non_neg_integer(),
    shares = [] :: [binary()],
    provider_id :: binary(),
    owner_id :: binary(),
    fully_replicated :: undefined | boolean(),
    nlink :: undefined | non_neg_integer()
}).

-record(xattr, {
    name :: binary(),
    value :: term()
}).

-endif.
