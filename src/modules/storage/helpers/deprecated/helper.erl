%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides a synchronous interface to the helpers NIF library.
%%% It wraps {@link helpers_nif} module by calling its functions and awaiting
%%% results.
%%% @end
%%%-------------------------------------------------------------------
-module(helper).
-author("Krzysztof Trzepla").

-include("proto/oneclient/fuse_messages.hrl").

%% API
-export([new_helper/3]).


%%%===================================================================
%%% API
%%%===================================================================


%% TODO rm
-spec new_helper(helper_config:name(), helper_config:args(), helper_config:user_ctx()) -> {ok, helper_config:t()}.
new_helper(HelperName, Args, AdminCtx) ->
    BaseAdminCtx = default_admin_ctx(HelperName),
    FullAdminCtx = maps:merge(BaseAdminCtx, AdminCtx),
%%    ok = helper_params:validate_args(HelperName, Args),
%%    ok = helper_params:validate_user_ctx(HelperName, FullAdminCtx),

    {ok, #helper_config{
        name = HelperName,
        args = Args,
        admin_ctx = FullAdminCtx
    }}.


%% @private
default_admin_ctx(HelperName) when
    HelperName == ?POSIX_HELPER_NAME;
    HelperName == ?NULL_DEVICE_HELPER_NAME;
    HelperName == ?NFS_HELPER_NAME;
    HelperName == ?GLUSTERFS_HELPER_NAME ->
    #{<<"uid">> => <<"0">>, <<"gid">> => <<"0">>};

default_admin_ctx(_) ->
    #{}.
