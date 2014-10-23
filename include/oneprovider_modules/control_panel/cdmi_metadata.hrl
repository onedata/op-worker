%% ===================================================================
%% @author Malgorzata Plazek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This file contains common macros and records for cdmi metadata operations.
%% @end
%% ===================================================================

-define(default_storage_system_metadata,
    [<<"cdmi_size">>, <<"cdmi_ctime">>, <<"cdmi_atime">>, <<"cdmi_mtime">>, <<"cdmi_owner">>, <<"cdmi_acl">>]).

-define(user_metadata_forbidden_prefix, <<"cdmi_">>).