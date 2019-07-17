%%%-------------------------------------------------------------------
%%% @author Łukasz Opioła
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions for REST.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(CDMI_HRL).
-define(CDMI_HRL, 1).

%% Endpoint used to make cdmi operations
-define(CDMI_ID_PATH, "/cdmi/cdmi_objectid/:id/[...]").
-define(CDMI_PATH, "/cdmi/[...]").


%% CAPABILITIES

%% Paths for all cdmi capability containers (they can be referred by those paths)
-define(ROOT_CAPABILITY_PATH, <<"cdmi_capabilities/">>).
-define(CONTAINER_CAPABILITY_PATH, <<"cdmi_capabilities/container/">>).
-define(DATAOBJECT_CAPABILITY_PATH, <<"cdmi_capabilities/dataobject/">>).

% Fake datastore guids of cdmi capability containers, necessary for objectid generation
-define(ROOT_CAPABILITY_GUID, base64:encode(<<"00000000000000000000000000000001">>)).
-define(CONTAINER_CAPABILITY_GUID, base64:encode(<<"00000000000000000000000000000002">>)).
-define(DATAOBJECT_CAPABILITY_GUID, base64:encode(<<"00000000000000000000000000000003">>)).

% Cdmi objectIDs for all cdmi capability containers
% equivalent of running:
% begin {ok, Id__} = file_id:guid_to_objectid(?ROOT_CAPABILITY_GUID), Id__ end).
-define(ROOT_CAPABILITY_ID,
    <<"0000000000208CA83030303030303030303030303030303030303030303030303030303030303031">>
).
-define(CONTAINER_CAPABILITY_ID,
    <<"0000000000208DE83030303030303030303030303030303030303030303030303030303030303032">>
).
-define(DATAOBJECT_CAPABILITY_ID,
    <<"0000000000204D293030303030303030303030303030303030303030303030303030303030303033">>
).

%% List of general cdmi system capabilities
%% CDMI documentation: chapter 12.1.1 and table 100.
-define(ROOT_CAPABILITY_MAP, #{
    <<"cdmi_dataobjects">> => <<"true">>,
    <<"cdmi_security_access_control">> => <<"true">>,
    <<"cdmi_object_move_from_local">> => <<"true">>,
    <<"cdmi_object_copy_from_local">> => <<"true">>,
    <<"cdmi_object_access_by_ID">> => <<"true">>
}).

%% List of cdmi container capabilites
%% Documentation: chapters 12.1.2, 12.1.4 and tables 101, 103
-define(CONTAINER_CAPABILITY_MAP, #{
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

%% List of cdmi object capabilites
%% Documentation: chapters 12.1.2, 12.1.3 and tables 101, 102
-define(DATAOBJECT_CAPABILITY_MAP, #{
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

-endif.
