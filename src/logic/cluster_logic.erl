%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating od_cluster records synchronized
%%% via Graph Sync. Requests are delegated to gs_client_worker, which decides
%%% if they should be served from cache or handled by Onezone.
%%% NOTE: This is the only valid way to interact with od_cluster records, to
%%% ensure consistency, no direct requests to datastore or OZ REST should
%%% be performed.
%%% @end
%%%-------------------------------------------------------------------
-module(cluster_logic).
-author("Lukasz Opiola").

-include("modules/fslogic/fslogic_common.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include_lib("ctool/include/onedata.hrl").
-include_lib("ctool/include/logging.hrl").

-export([update_version_info/3, upload_op_worker_gui/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Updates the version info of this Oneprovider worker in the corresponding
%% cluster in Onezone. May return ?ERROR_BAD_VALUE_NOT_ALLOWED if the requested
%% GUI version is not present in Onezone (in such case, GUI must be uploaded
%% before updating the version info).
%% @end
%%--------------------------------------------------------------------
-spec update_version_info(Release :: binary(), Build :: binary(), GuiHash :: binary()) ->
    ok | gs_protocol:error().
update_version_info(Release, Build, GuiHash) ->
    ClusterId = oneprovider:get_id(),
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = update,
        gri = #gri{type = od_cluster, id = ClusterId, aspect = instance},
        data = #{
            <<"workerVersion">> => #{
                <<"release">> => Release,
                <<"build">> => Build,
                <<"gui">> => GuiHash
            }
        }
    }).


%%--------------------------------------------------------------------
%% @doc
%% Uploads OP worker GUI static package to Onezone.
%% @end
%%--------------------------------------------------------------------
-spec upload_op_worker_gui(file:filename_all()) -> ok | {error, gui_upload_failed}.
upload_op_worker_gui(PackagePath) ->
    ClusterId = oneprovider:get_id(),
    ServiceShortname = onedata:service_shortname(?OP_WORKER),
    Result = oz_endpoint:request(
        provider,
        str_utils:format("/~s/~s/gui-upload", [ServiceShortname, ClusterId]),
        post,
        {multipart, [{file, str_utils:to_binary(PackagePath)}]},
        [{endpoint, gui}]
    ),
    case Result of
        {ok, 200, _, _} ->
            ok;
        Other ->
            ?error("Uploading GUI package to Onezone failed with result: ~p", [Other]),
            {error, upload_failed}
    end.