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

-type uuid() :: string().
-type path() :: string().

-type file_path() :: {absolute_path, Path :: path()} | {relative_path, Path :: path(), RootUUID :: uuid()}.
-type file() :: file_path() | {uuid, FileUUID :: uuid()}.
-type file_info() :: #file{}.
-type file_doc() :: #veil_document{record :: #file{}}.

-type veil_doc() :: #veil_document{}.

-endif.