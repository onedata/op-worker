%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This header provides common definitions for cdmi_capabilities,
%%% such as lists of capabilities, ids and paths of main capability containers:
%%% "cdmi_capabilities/", "cdmi_capabilities/container/", "cdmi_capabilities/dataobject/"
%%% @end
%%%-------------------------------------------------------------------

%% the default json response for capability object will contain this entities, they can be choosed selectively by appending '?name1;name2' list to the requested url
-define(default_get_capability_opts, [<<"objectType">>, <<"objectID">>, <<"objectName">>, <<"parentURI">>, <<"parentID">>, <<"capabilities">>, <<"childrenrange">>, <<"children">>]).

%% List of general cdmi system capabilites
%% CDMI documentation: chapter 12.1.1 and table 100.
-define(root_capability_map, #{
    <<"cdmi_dataobjects">> => <<"true">>,
    <<"cdmi_security_access_control">> => <<"true">>,
    <<"cdmi_object_move_from_local">> => <<"true">>,
    <<"cdmi_object_copy_from_local">> => <<"true">>,
    <<"cdmi_object_access_by_ID">> => <<"true">>
}).

%% List of cdmi object capabilites
%% Documentation: chapters 12.1.2, 12.1.3 and tables 101, 102
-define(dataobject_capability_list, #{
    <<"cdmi_acl">> => <<"true">>,
    <<"cdmi_size">> => <<"true">>,
    <<"cdmi_ctime">> => <<"true">>,
    <<"cdmi_atime">> => <<"true">>,
    <<"cdmi_mtime">> => <<"true">>,
    <<"cdmi_read_value">> => <<"true">>,
    <<"cdmi_read_value_range">> => <<"true">>,
    <<"cdmi_read_metadata">> => <<"true">>,
    <<"cdmi_modify_value">> => <<"true">>,
    <<"cdmi_modify_value_range">> => <<"true">>,
    <<"cdmi_modify_metadata">> => <<"true">>,
    <<"cdmi_delete_dataobject">> => <<"true">>
}).

%% List of cdmi container capabilites
%% Documentation: chapters 12.1.2, 12.1.4 and tables 101, 103
-define(container_capability_list, #{
    <<"cdmi_acl">> => <<"true">>,
    <<"cdmi_size">> => <<"true">>,
    <<"cdmi_ctime">> => <<"true">>,
    <<"cdmi_atime">> => <<"true">>,
    <<"cdmi_mtime">> => <<"true">>,
    <<"cdmi_list_children">> => <<"true">>,
    <<"cdmi_list_children_range">> => <<"true">>,
    <<"cdmi_read_metadata">> => <<"true">>,
    <<"cdmi_modify_metadata">> => <<"true">>,
    <<"cdmi_create_dataobject">> => <<"true">>,
    <<"cdmi_create_container">> => <<"true">>,
    <<"cdmi_delete_container">> => <<"true">>,
    <<"cdmi_move_container">> => <<"true">>,
    <<"cdmi_copy_container">> => <<"true">>,
    <<"cdmi_move_dataobject">> => <<"true">>,
    <<"cdmi_copy_dataobject">> => <<"true">>
}).

%% Paths for all cdmi capability containers (they can be refered by those paths)
-define(root_capability_path, <<"cdmi_capabilities/">>).
-define(container_capability_path, <<"cdmi_capabilities/container/">>).
-define(dataobject_capability_path, <<"cdmi_capabilities/dataobject/">>).

% Fake datastore uuids, necessary for objectid generation
-define(root_capability_uuid, base64:encode(<<"00000000000000000000000000000001">>)).
-define(container_capability_uuid, base64:encode(<<"00000000000000000000000000000002">>)).
-define(dataobject_capability_uuid, base64:encode(<<"00000000000000000000000000000003">>)).

% Cdmi objectIDs for all cdmi capability containers
% equivalent of running:
% begin {ok, Id__} = cdmi_id:uuid_to_objectid(?root_capability_uuid), Id__ end).
-define(root_capability_id,
    <<"0000000000208CA83030303030303030303030303030303030303030303030303030303030303031">>).
-define(container_capability_id,
    <<"0000000000208DE83030303030303030303030303030303030303030303030303030303030303032">>).
-define(dataobject_capability_id,
    <<"0000000000204D293030303030303030303030303030303030303030303030303030303030303033">>).

%% Proplist that provides mapping between path and capability name
-define(CapabilityNameByPath, [
    {?root_capability_path, root},
    {?container_capability_path, container},
    {?dataobject_capability_path, dataobject}]).

%% Proplist that provides mapping between objectid and capability path
-define(CapabilityPathById, [
    {?root_capability_id, filename:absname(<<"/", (?root_capability_path)/binary>>)},
    {?container_capability_id, filename:absname(<<"/", (?container_capability_path)/binary>>)},
    {?dataobject_capability_id, filename:absname(<<"/", (?dataobject_capability_path)/binary>>)}
]).

