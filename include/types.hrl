%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2015, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This header file contains declarations of types used across the project.
%% @end
%% ===================================================================

% TODO which is better - hold all types in one hrl and include it everywhere?
% TODO or create many hrls and group the types
% TODO those types might also be exported from .erl modules

%%--------------------------------------------------------------------
%% IDs of entities
-type file_id() :: binary().
-type group_id() :: binary().
-type user_id() :: binary().
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Types connected with files
-type file_path() :: binary().
-type file_handle() :: binary().
-type file_name() :: binary().
-type file_id_or_path() :: {uuid, file_id()} | {path, file_path()}.
-type file_key() :: {uuid, file_id()} | {path, file_path()} | {handle, file_handle()}.
-type open_type() :: read | write | rdwr.
-type perms_octal() :: non_neg_integer().
-type permission_type() :: root | owner | delete | read | write | execute | rdwr.
-type file_attributes() :: term(). % TODO should be a proper record
-type xattr_key() :: binary().
-type xattr_value() :: binary().
-type access_control_entity() :: term(). % TODO should be a proper record
-type block_range() :: term(). % TODO should be a proper record
-type share_id() :: binary().
%%--------------------------------------------------------------------

%% --------------------------------------------------------------------
%% Misc
-type error_reply() :: {error, term()}.
%%--------------------------------------------------------------------

