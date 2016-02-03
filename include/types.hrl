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
% TODO those types might also be exported from .erl modules <----- +1

-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%%--------------------------------------------------------------------
%% IDs of entities
-type file_uuid() :: file_meta:uuid().
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Types connected with files
-type file_path() :: file_meta:path().
-type file_handle() :: logical_file_manager:handle().
-type file_name() :: file_meta:name().
-type file_id_or_path() :: {uuid, file_uuid()} | {path, file_path()}.
-type file_key() :: fslogic_worker:file() | {handle, file_handle()}.
-type open_mode() :: helpers:open_mode().
-type permission_type() :: root | owner | delete | read | write | execute | rdwr.
-type file_attributes() :: #file_attr{}.
-type access_control_entity() :: #accesscontrolentity{}.
-type block_range() :: term(). % TODO should be a proper record
-type share_id() :: binary().
-type transfer_encoding() :: binary(). % <<"utf-8">> | <<"base64">>
-type cdmi_completion_status() :: binary(). % <<"Completed">> | <<"Processing">> | <<"Error">>
-type mimetype() :: binary().
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Misc
-type error_reply() :: {error, term()}.
%%--------------------------------------------------------------------

