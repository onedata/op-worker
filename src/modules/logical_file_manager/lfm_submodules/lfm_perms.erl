%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module performs permissions-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_perms).

-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([set_perms/3]).


%%%===================================================================
%%% API
%%%===================================================================


-spec set_perms(session:id(), lfm:file_key(), file_meta:posix_permissions()) ->
    ok | lfm:error_reply().
set_perms(SessId, FileKey, NewPerms) ->
    Guid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),

    remote_utils:call_fslogic(SessId, file_request, Guid,
        #change_mode{mode = NewPerms},
        fun(_) -> ok end
    ).
