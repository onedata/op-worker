%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for managing file metadata (requests are delegated to middleware_worker).
%%% @end
%%%-------------------------------------------------------------------
-module(mi_file_metadata).
-author("Bartosz Walkowicz").

-include("modules/fslogic/file_distribution.hrl").

%% API
-export([get_distribution/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_distribution(session:id(), lfm:file_key()) ->
    file_distribution:get_result() | no_return().
get_distribution(SessionId, FileKey) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, do_not_resolve_symlink),
    middleware_worker:check_exec(SessionId, FileGuid, #file_distribution_get_request{}).
