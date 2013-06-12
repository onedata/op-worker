%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: DAO types definitions
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

-type fd() :: uuid().
-type fd_info() :: #file_descriptor{}.
-type fd_doc() :: #veil_document{record :: #file_descriptor{}}.

-type veil_doc() :: #veil_document{}.

-endif.