%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains common macros and records for control_panel modules.
%% @end
%% ===================================================================

-ifndef(CONTROL_PANEL_COMMON_HRL).
-define(CONTROL_PANEL_COMMON_HRL, 1).


-include("registered_names.hrl").

%% Include common gui hrl from ctool
-include_lib("ctool/include/gui/common.hrl").
-include_lib("dao/include/common.hrl").

% Relative suffix of GUI address, leading to shared files
-define(shared_files_download_path, "/share/").

% Identifier for requests of shared files
-define(shared_files_request_type, shared_files).

% Relative suffix of GUI address, leading to user content download
-define(user_content_download_path, "/user_content").

% Identifier for requests of user content
-define(user_content_request_type, user_content).

% Relative suffix of GUI address, leading to file upload service
-define(file_upload_path, "/upload").

% Relative suffix of GUI address, leading to privacy policy page
-define(privacy_policy_url, "/privacy_policy").

% Relative suffix of GUI address, leading to the page where oneclient can be downloaded
-define(client_download_page_url, "/client_download").

% Name of parameter sent to logout page that verifies user logout
-define(logout_token, <<"logout_token">>).

% Macros used as ids of errors that can appear on GUI pages
-define(error_user_content_not_logged_in, "uc_not_logged_in").
-define(error_user_content_file_not_found, "uc_file_not_found").
-define(error_user_permission_denied, "uc_permission_denied").
-define(error_shared_file_not_found, "sh_file_not_found").
-define(error_internal_server_error, "internal_server_error").
-define(error_openid_invalid_request, "openid_invalid_request").
-define(error_openid_auth_invalid, "openid_auth_invalid").
-define(error_openid_no_connection, "openid_no_connection").
-define(error_openid_login_error, "openid_login_error").
-define(error_login_dir_creation_error, "login_dir_creation_error").
-define(error_login_dir_chown_error, "login_dir_chown_error").
-define(error_authentication, "error_authentication").
-define(error_space_permission_denied, "space_permission_denied").
-define(error_space_not_found, "space_not_found").
-define(error_group_permission_denied, "group_permission_denied").
-define(error_group_not_found, "group_not_found").

% Custom upload element
-record(oneprovider_upload, {?ELEMENT_BASE(oneprovider_upload),
    subscriber_pid, % Required to get start / finish reports.
    target_dir = <<"/">>
}).

-endif.

