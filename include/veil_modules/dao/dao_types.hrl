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
-include_lib("veil_modules/dao/dao.hrl").

-type uuid() :: string(). %% Pattern: "^[0-9a-f]+$"
-type path() :: string(). %% Pattern: "^/?(.+/)*[.+]?$" (starting with ?PATH_SEPARATOR is optional)

-type file_path() :: {absolute_path, Path :: path()} | {relative_path, Path :: path(), RootUUID :: uuid()}
                   | {Path :: path(), RootUUID :: uuid()} | uuid().
-type file() :: file_path() | {uuid, FileUUID :: uuid()}.
-type file_info() :: #file{}.
-type file_doc() :: #veil_document{record :: #file{}}.

-type file_criteria() :: #file_criteria{}.

-type fd() :: uuid().
-type fd_select() :: {by_file, File :: file()} | {by_file_n_owner, {File :: file(), Owner :: string()}}.
-type fd_info() :: #file_descriptor{}.
-type fd_doc() :: #veil_document{record :: #file_descriptor{}}.

-type storage_doc() :: #veil_document{record :: #storage_info{}}.

-type veil_doc() :: #veil_document{}.

-type user() :: uuid().
-type user_info() :: #user{}.
-type user_doc() :: #veil_document{record :: #user{}}.
-type user_key() :: {login, Login :: string()} |
                    {email, Email :: string()} |
                    {uuid, UUID :: uuid()} |
                    {dn, DN :: string()} |
                    {unverified_dn, DN :: string()}.

-type quota() :: uuid().
-type quota_info() :: #quota{}.
-type quota_doc() :: #veil_document{record :: #quota{}}.

-type file_share() :: uuid().
-type file_share_info() :: #share_desc{}.
-type file_share_doc() :: #veil_document{record :: #share_desc{}}.

-endif.