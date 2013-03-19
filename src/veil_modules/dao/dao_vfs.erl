%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module gives high level DB API which contain veil file system specific methods.
%% All DAO API functions should not be called directly. Call dao:handle(_, {vfs, MethodName, ListOfArgs) instead.
%% See dao:handle/2 for more details.
%% @end
%% ===================================================================
-module(dao_vfs).

-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/couch_db.hrl").

%% API - File system management
-export([del_file/2, list_dir/2, lock_file/3, rename_file/3, unlock_file/3]).

-ifdef(TEST).
-compile([export_all]).
-endif.

%% ===================================================================
%% API functions
%% ===================================================================



%% list_dir/2
%% ====================================================================
%% @doc Lists all files from specified directory owned by specified user.
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% Not yet implemented. This is placeholder/template method only!
%% @end
-spec list_dir(UserID :: string(), DirID :: string()) -> not_yet_implemented.
%% ====================================================================
list_dir(_UserID, _DirID) ->
    not_yet_implemented.

%% lock_file/3
%% ====================================================================
%% @doc Puts a read/write lock on specified file owned by specified user.
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% Not yet implemented. This is placeholder/template method only!
%% @end
-spec lock_file(UserID :: string(), FileID :: string(), Mode :: write | read) -> not_yet_implemented.
%% ====================================================================
lock_file(_UserID, _FileID, _Mode) ->
    not_yet_implemented.

%% unlock_file/3
%% ====================================================================
%% @doc Takes off a read/write lock on specified file owned by specified user.
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% Not yet implemented. This is placeholder/template method only!
%% @end
-spec unlock_file(UserID :: string(), FileID :: string(), Mode :: write | read) -> not_yet_implemented.
%% ====================================================================
unlock_file(_UserID, _FileID, _Mode) ->
    not_yet_implemented.


%% rename_file/3
%% ====================================================================
%% @doc Renames specified file owned by specified user to NewName.
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% Not yet implemented. This is placeholder/template method only!
%% @end
-spec rename_file(UserID :: string(), FileID :: string(), _NewName :: string()) -> not_yet_implemented.
%% ====================================================================
rename_file(_UserID, _FileID, _NewName) ->
    not_yet_implemented.

%% del_file/2
%% ====================================================================
%% @doc Deletes specified file owned by specified user.
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% Not yet implemented. This is placeholder/template method only!
%% @end
-spec del_file(UserID :: string(), FileID :: string()) -> not_yet_implemented.
%% ====================================================================
del_file(_UserID, _FileID) ->
    not_yet_implemented.

    
%% ===================================================================
%% Internal functions
%% ===================================================================
    
