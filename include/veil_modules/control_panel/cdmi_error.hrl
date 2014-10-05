%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This header provides common error names that can be throwed in cdmi
%% modules. Such errors are handled in cdmi_error.erl
%% ===================================================================

-define(moved_permanently, moved_permanently).

-define(duplicated_body_fields, duplicated_body_fields).
-define(conflicting_body_fields, conflicting_body_fields).
-define(invalid_objectid, invalid_objectid).
-define(invalid_content_type, invalid_content_type).
-define(invalid_range, invalid_range).
-define(invalid_childrenrange, invalid_childrenrange).
-define(no_version_given, no_version_given).
-define(invalid_base64, invalid_base64).
-define(unsupported_version, unsupported_version).
-define(malformed_request, malformed_request).

-define(invalid_json, invalid_json).
-define(invalid_token, invalid_token).
-define(invalid_cert, invalid_cert).
-define(no_certificate_chain_found, no_certificate_chain_found).
-define(user_unknown, user_unknown).

-define(space_dir_delete, group_dir_delete).
-define(forbidden, forbidden).

-define(not_found, not_found).
-define(parent_not_found, parent_not_found).

-define(put_container_conflict, put_container_conflict).

-define(get_attr_unknown_error, get_attr_unknown_error).
-define(file_delete_unknown_error, file_delete_unknown_error).
-define(put_container_unknown_error, put_container_unknown_error).
-define(dir_delete_unknown_error, dir_delete_unknown_error).
-define(state_init_error, state_init_error).
-define(put_object_unknown_error, put_object_unknown_error).
-define(write_object_unknown_error, write_object_unknown_error).