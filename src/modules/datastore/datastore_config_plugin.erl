%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines datastore config related to op_worker.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_config_plugin).
-author("Michal Zmuda").

-behaviour(datastore_config_behaviour).

%% datastore_config_behaviour callbacks
-export([models/0]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% {@link datastore_config_behaviour} callback models/0.
%% @end
%%--------------------------------------------------------------------
-spec models() -> Models :: [model_behaviour:model_type()].
models() -> [
    subscriptions_state,
    subscription,
    session,
    onedata_user,
    onedata_group,
    identity,
    file_meta,
    storage,
    file_location,
    xattr,
    provider_info,
    space_info,
    space_storage,
    ceph_user,
    s3_user,
    dbsync_state,
    files_to_chown,
    posix_user,
    message_id,
    space_quota,
    monitoring_state,
    open_file,
    monitoring_init_state,
    swift_user,
    file_consistency
].