%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This header provides common definitions for cdmi_capabilities,
%% such as lists of capabilities, ids and paths of main capability containers:
%% "cdmi_capabilities/", "cdmi_capabilities/container/", "cdmi_capabilities/dataobject/"
%% @end
%% ===================================================================

%% the default json response for capability object will contain this entities, they can be choosed selectively by appending '?name1;name2' list to the requested url
-define(default_get_capability_opts,[<<"objectType">>,<<"objectID">>,<<"objectName">>,<<"parentURI">>,<<"parentID">>,<<"capabilities">>,<<"childrenrange">>,<<"children">>]).

-define(root_capability_list,[
    {<<"cdmi_dataobjects">>, <<"true">>},
    {<<"cdmi_object_access_by_ID">>, <<"true">>}
]).
-define(container_capability_list,[
    {<<"cdmi_size">>,<<"true">>},
    {<<"cdmi_ctime">>,<<"true">>},
    {<<"cdmi_atime">>,<<"true">>},
    {<<"cdmi_mtime">>,<<"true">>},
    {<<"cdmi_read_value">>,<<"true">>},
    {<<"cdmi_read_value_range">>,<<"true">>},
    {<<"cdmi_read_metadata">>,<<"true">>},
    {<<"cdmi_modify_value">>,<<"true">>},
    {<<"cdmi_modify_value_range">>,<<"true">>},
    {<<"cdmi_delete_dataobject">>,<<"true">>}
]).
-define(dataobject_capability_list,[
    {<<"cdmi_size">>,<<"true">>},
    {<<"cdmi_ctime">>,<<"true">>},
    {<<"cdmi_atime">>,<<"true">>},
    {<<"cdmi_mtime">>,<<"true">>},
    {<<"cdmi_list_children">>,<<"true">>},
    {<<"cdmi_read_metadata">>,<<"true">>},
    {<<"cdmi_create_dataobject">>,<<"true">>},
    {<<"cdmi_create_container">>,<<"true">>},
    {<<"cdmi_delete_container">>,<<"true">>}
]).

-define(root_capability_path,"cdmi_capabilities/").
-define(container_capability_path,"cdmi_capabilities/container/").
-define(dataobject_capability_path,"cdmi_capabilities/dataobject/").

% these are fake uuids, necessary for objectid generation
-define(root_capability_uuid,"0000000000000001").
-define(container_capability_uuid,"0000000000000002").
-define(dataobject_capability_uuid,"0000000000000003").

-define(root_capability_id,cdmi_id:uuid_to_objectid(?root_capability_uuid)).
-define(container_capability_id,cdmi_id:uuid_to_objectid(?container_capability_uuid)).
-define(dataobject_capability_id,cdmi_id:uuid_to_objectid(?dataobject_capability_uuid)).

-define(CapabilityNameByPath, [
    {?root_capability_path, root},
        {?container_capability_path, container},
        {?dataobject_capability_path, dataobject}]).

-define(CapabilityPathById, [
    {?root_capability_id, ?root_capability_path},
        {?container_capability_id, ?container_capability_path},
        {?dataobject_capability_id, ?dataobject_capability_path}
]).

