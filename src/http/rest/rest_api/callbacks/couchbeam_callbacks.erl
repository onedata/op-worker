%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc Callbacks registered in couchbeam.
%%% @end
%%%--------------------------------------------------------------------
-module(couchbeam_callbacks).
-author("Tomasz Lichon").

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/dbsync/common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").

%% API
-export([notify_function/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Callback for receiving notifications from couchbase changes stream.
%% @end
%%--------------------------------------------------------------------
-spec notify_function(pid(), reference()) -> function().
notify_function(Pid, Ref) ->
    fun
        (_, stream_ended, _) ->
            Pid ! {Ref, stream_ended};
        (Seq, Doc = #document{value = Value}, Model)
            when is_record(Value, file_meta); is_record(Value, custom_metadata) ->
            Pid ! {Ref, #change{seq = Seq, doc = Doc, model = Model}};
        (_Seq, _Doc, _Model) ->
            ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================