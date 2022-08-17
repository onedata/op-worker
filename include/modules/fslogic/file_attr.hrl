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
    guid :: fslogic_worker:file_guid(),
    name :: file_meta:name(),
    mode :: file_meta:mode(),
    parent_guid :: undefined | fslogic_worker:file_guid(),
    uid = 0 :: non_neg_integer(),
    gid = 0 :: non_neg_integer(),
    atime = 0 :: times:a_time(),
    mtime = 0 :: times:m_time(),
    ctime = 0 :: times:c_time(),
    type :: ?REGULAR_FILE_TYPE | ?DIRECTORY_TYPE | ?LINK_TYPE | ?SYMLINK_TYPE,
    size = 0 :: undefined | file_meta:size(),
    shares = [] :: [od_share:id()],
    provider_id :: od_provider:id(),
    owner_id :: od_user:id(),
    fully_replicated :: undefined | boolean(),
    nlink :: undefined | non_neg_integer(),
    % Listing index can be used to list dir children starting from this file
    index :: file_listing:index()
}).

-record(xattr, {
    name :: binary(),
    value :: term()
}).

-endif.
