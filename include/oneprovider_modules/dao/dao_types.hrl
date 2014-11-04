%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc DAO types definitions
%% @end
%% ===================================================================

-ifndef(DAO_TYPES_HRL).
-define(DAO_TYPES_HRL, 1).
-include_lib("oneprovider_modules/dao/dao.hrl").

-type uuid() :: string(). %% Pattern: "^[0-9a-f]+$"
-type path() :: string(). %% Pattern: "^/?(.+/)*[.+]?$" (starting with ?PATH_SEPARATOR is optional)

-type db_doc() :: #db_document{}.
-type db_doc(Record) :: #db_document{record :: Record}.

-type file_path() :: {absolute_path, Path :: path()} | {relative_path, Path :: path(), RootUUID :: uuid()}
                   | {Path :: path(), RootUUID :: uuid()} | uuid().
-type file() :: file_path() | {uuid, FileUUID :: uuid()}.
-type file_info() :: #file{}.
-type file_doc() :: db_doc(file_info()).

-type file_location_info() :: #file_location{}.
-type file_location_doc() :: db_doc(file_location_info()).

-type file_block_info() :: #file_block{}.
-type file_block_doc() :: db_doc(file_block_info()).

-type remote_location_info() :: #remote_location{}.
-type remote_location_doc() :: db_doc(remote_location_info()).

-type file_criteria() :: #file_criteria{}.

-type fd() :: uuid().
-type fd_select() :: {by_file, File :: file()} | {by_file_n_owner, {File :: file(), Owner :: string()}}.
-type fd_info() :: #file_descriptor{}.
-type fd_doc() :: db_doc(fd_info()).

-type storage_doc() :: #db_document{record :: #storage_info{}}.

-type user() :: uuid().
-type user_info() :: #user{}.
-type user_doc() :: db_doc(user_info()).
-type user_key() :: {login, Login :: string()} |
                    {global_id, GlobalID :: string()} |
                    {name, Name :: string()} |
                    {email, Email :: string()} |
                    {uuid, UUID :: uuid()} |
                    {dn, DN :: string()} |
                    {unverified_dn, DN :: string()}.

-type quota() :: uuid().
-type quota_info() :: #quota{}.
-type quota_doc() :: #db_document{record :: #quota{}}.

-type file_share() :: uuid().
-type file_share_info() :: #share_desc{}.
-type file_share_doc() :: db_doc(file_share_info()).

-type cookie() :: uuid().
-type cookie_info() :: #session_cookie{}.
-type cookie_doc() :: db_doc(cookie_info()).

-type group() :: uuid().
-type group_info() :: #group_details{}.
-type group_doc() :: db_doc(group_info()).

-endif.
