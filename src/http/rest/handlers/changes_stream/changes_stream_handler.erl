%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2016-2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for streaming changes happening to `file_meta`, `file_location`,
%%% `times` or `custom_metadata` in scope of given space.
%%%
%%% Possible fields to observe are shown below.
%%%
%%% fileMeta:
%%%     - name
%%%     - type
%%%     - mode
%%%     - owner
%%%     - group_owner
%%%     - provider_id
%%%     - shares
%%%     - deleted
%%%
%%% fileLocation:
%%%     - provider_id
%%%     - storage_id
%%%     - size
%%%     - space_id
%%%     - storage_file_created
%%%
%%% times:
%%%     - atime
%%%     - mtime
%%%     - ctime
%%%
%%% customMetadata:
%%%     - onedata_json
%%%     - onedata_rdf
%%%     - onedata_keyvalue
%%%     ...
%%%
%%% In case of `file_meta`, `file_location` and `times` fields corresponds
%%% one to one to those held in records. As for `custom_metadata` elements
%%% from `value` map field in record can be requested. One additional metakey
%%% named `onedata_keyvalue` can be used to observe all elements beside
%%% `onedata_json` and `onedata_rdf`.
%%%
%%% To open stream user must specify records to observe and either
%%% fields to return (`fields`), fields for which check existence
%%% (`exists` - only possible for `customMetadata`),
%%% whether information about this record should be send always or
%%% on this record changes only (`always` boolean flag with default value
%%% being 'false' - not sending information on other docs changes).
%%% Optionally `triggers`, that is list of documents whose changes
%%% triggers sending events, can also be specified.
%%%
%%% <record>:
%%%     [fields: <fields>]
%%%     [exists: <fields>]
%%%     [always: boolean()]
%%%
%%% Response will include beside requested information also additional metadata
%%% like fileId, filePath, seq and mutators, rev, deleted, changed for each doc.
%%%
%%% EXAMPLE REQUEST:
%%%
%%% triggers:
%%%     - fileMeta,
%%%     - times
%%% fileMeta:
%%%     fields: [owner]
%%% customMetadata:
%%%     fields: [onedata_rdf]
%%%     exists: [onedata_json]
%%%     always: true
%%%
%%% EXAMPLE RESPONSE:
%%%
%%% fileId: 00000000002C66ED677569642361626562383736303665323765313
%%% filePath: space1/my/file
%%% seq: 100
%%% fileMeta:
%%%     rev: 2-c500a5eb026d9474429903d47841f9c5
%%%     mutators: ["p1.1542789098.test"]
%%%     changed: true
%%%     deleted: false
%%%     fields:
%%%         owner: john
%%% customMetadata:
%%%     rev: 1-09f941b4e8452ef6a244c5181d894814
%%%     mutators: ["p1.1542789098.test"]
%%%     changed: false
%%%     deleted: false
%%%     exists:
%%%         onedata_rdf: true
%%%     fields:
%%%         onedata_json:
%%%             name1: value1
%%%             name2: value2
%%%
%%% @end
%%%--------------------------------------------------------------------
-module(changes_stream_handler).
-author("Tomasz Lichon").
-author("Bartosz Walkowicz").

-include("http/changes_stream.hrl").
-include("middleware/middleware.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").

%% API
-export([
    init/2, terminate/3,
    allowed_methods/2, is_authorized/2,
    content_types_accepted/2
]).

%% resource functions
-export([stream_space_changes/2]).

%% for tests
-export([init_stream/1]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Cowboy callback function.
%% Initialize the state for this request.
%% @end
%%--------------------------------------------------------------------
-spec init(cowboy_req:req(), term()) ->
    {cowboy_rest, cowboy_req:req(), map()}.
init(Req, _Opts) ->
    {cowboy_rest, Req, #{}}.


%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), cowboy_req:req(), map()) -> ok.
terminate(_, _, #{changes_stream := Stream, loop_pid := Pid, ref := Ref}) ->
    couchbase_changes:cancel_stream(Stream),
    Pid ! {Ref, stream_ended};
terminate(_, _, #{changes_stream := Stream}) ->
    couchbase_changes:cancel_stream(Stream);
terminate(_, _, #{}) ->
    ok.


%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(cowboy_req:req(), map() | {error, term()}) ->
    {[binary()], cowboy_req:req(), map()}.
allowed_methods(Req, State) ->
    {[<<"POST">>], Req, State}.


%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(cowboy_req:req(), map()) ->
    {true | {false, binary()} | halt, cowboy_req:req(), map()}.
is_authorized(Req, State) ->
    AuthCtx = #http_auth_ctx{
        interface = rest,
        data_access_caveats_policy = disallow_data_access_caveats
    },
    case http_auth:authenticate(Req, AuthCtx) of
        {ok, ?USER(UserId, SessionId) = Auth} ->
            case authorize(Req, Auth) of
                ok ->
                    {true, Req, State#{user_id => UserId, auth => SessionId}};
                {error, _} = Error ->
                    {stop, http_req:send_error(Error, Req), State}
            end;
        {ok, ?GUEST} ->
            {stop, http_req:send_error(?ERR_UNAUTHORIZED(?err_ctx(), undefined), Req), State};
        {error, _} = Error ->
            {stop, http_req:send_error(Error, Req), Req}
    end.


%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_accepted(cowboy_req:req(), map()) ->
    {[{binary(), atom()}], cowboy_req:req(), map()}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/json">>, stream_space_changes}
    ], Req, State}.


%%%===================================================================
%%% Content type handler functions
%%%===================================================================


%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/changes/metadata/:sid'
%% @doc
%% This method streams changes happening in space filtering only
%% relevant/requested ones.
%%
%% HTTP method: POST
%%
%% @param timeout Time of inactivity after which close stream.
%% @param last_seq
%%--------------------------------------------------------------------
-spec stream_space_changes(cowboy_req:req(), map()) ->
    {term(), cowboy_req:req(), map()}.
stream_space_changes(Req, State) ->
    try changes_stream_parser:parse_request(Req) of
        {Req2, ChangesMonitoringSpec} ->
            State2 = State#{changes_monitoring_spec => ChangesMonitoringSpec},
            State3 = ?MODULE:init_stream(State2),
            Req3 = cowboy_req:stream_reply(
                ?HTTP_200_OK, #{?HDR_CONTENT_TYPE => <<"application/json">>}, Req2
            ),
            stream_loop(Req3, State3),
            cowboy_req:stream_body(<<"">>, fin, Req3),

            {stop, Req3, State3}
    catch
        throw:Error ->
            {stop, http_req:send_error(Error, Req), State};
        Type:Message:Stacktrace ->
            ?error_stacktrace("Unexpected error in ~tp:process_request - ~tp:~tp", [
                ?MODULE, Type, Message
            ], Stacktrace),
            NewReq = cowboy_req:reply(?HTTP_500_INTERNAL_SERVER_ERROR, Req),
            {stop, NewReq, State}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec authorize(cowboy_req:req(), aai:auth()) -> ok | errors:error().
authorize(Req, ?USER(UserId) = Auth) ->
    SpaceId = cowboy_req:binding(sid, Req),

    case space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_CHANGES_STREAM) of
        true ->
            try
                GRI = #gri{type = op_metrics, id = SpaceId, aspect = changes},
                api_auth:check_authorization(Auth, ?OP_WORKER, create, GRI)
            catch
                _:_ ->
                    ?ERR_INTERNAL_SERVER_ERROR(?err_ctx(), undefined)
            end;
        false ->
            ?ERR_FORBIDDEN(?err_ctx())
    end.


%%--------------------------------------------------------------------
%% @doc
%% Init changes stream.
%% @end
%%--------------------------------------------------------------------
-spec init_stream(State :: map()) -> map().
init_stream(#{last_seq := Since, space_id := SpaceId, triggers := Triggers} = State) ->
    ?info("[ changes ]: Starting stream ~tp", [Since]),
    Ref = make_ref(),
    Pid = self(),

    % TODO VFS-5570
    % TODO VFS-6389 - maybe restart stream in case of node failure
    Node = datastore_key:any_responsible_node(SpaceId),
    {ok, Stream} = rpc:call(Node, couchbase_changes, stream, [
        <<"onedata">>,
        SpaceId,
        fun(Feed) -> notify(Pid, Ref, Triggers, Feed) end,
        [{since, Since}],
        [Pid]
    ]),

    State#{changes_stream => Stream, ref => Ref, loop_pid => Pid}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Listens for events and pushes them to the socket
%% @end
%%--------------------------------------------------------------------
-spec stream_loop(cowboy_req:req(), map()) -> ok.
stream_loop(Req, State = #{
    changes_stream := Stream,
    timeout := Timeout,
    ref := Ref,
    auth := SessionId,
    changes_monitoring_spec := ChangesMonitoringSpec
}) ->
    receive
        {Ref, stream_ended} ->
            ok;
        {Ref, ChangedDocs} when is_list(ChangedDocs) ->
            Stream ! {Ref, ok},
            UserCtx = user_ctx:new(SessionId),
            lists:foreach(fun(ChangedDoc) ->
                try
                    case changes_stream_processor:process_doc(UserCtx, ChangedDoc, ChangesMonitoringSpec) of
                        ok ->
                            ok;
                        {ok, Changes} ->
                            Response = json_utils:encode(Changes),
                            cowboy_req:stream_body(<<Response/binary, "\r\n">>, nofin, Req)
                    end
                catch Class:Reason:Stacktrace ->
                    % Can appear when document connected with deleted file_meta appears
                    ?debug_exception("Cannot stream change of ~tp", [ChangedDoc], Class, Reason, Stacktrace)
                end
            end, ChangedDocs),
            stream_loop(Req, State);
        Msg ->
            ?log_bad_request(Msg),
            stream_loop(Req, State)
    after
        Timeout ->
            % TODO VFS-4025 - is it always ok?
            ok
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Forwards changes feed to a streaming process.
%% @end
%%--------------------------------------------------------------------
-spec notify(pid(), reference(), changes_stream_processor:triggers(),
    {ok, [datastore:doc()] | datastore:doc() | end_of_stream} |
    {error, couchbase_changes:since(), term()}) -> ok.
notify(Pid, Ref, Triggers, {ok, {change, #document{} = Doc}}) ->
    case is_observed_doc(Doc, Triggers) of
        true ->
            call_changes_stream_handler(Pid, Ref, [Doc]);
        false ->
            ok
    end,
    ok;
notify(Pid, Ref, Triggers, {ok, Docs}) when is_list(Docs) ->
    case lists:filtermap(fun({change, Doc}) ->
        case is_observed_doc(Doc, Triggers) of
            true -> {true, Doc};
            false -> false
        end
    end, Docs) of
        [] ->
            ok;
        RelevantDocs ->
            call_changes_stream_handler(Pid, Ref, RelevantDocs)
    end,
    ok;
notify(Pid, Ref, _Triggers, {ok, end_of_stream}) ->
    Pid ! {Ref, stream_ended},
    ok;
notify(Pid, Ref, _Triggers, {error, _Seq, shutdown = Reason}) ->
    ?debug("Changes stream terminated due to: ~tp", [Reason]),
    Pid ! {Ref, stream_ended},
    ok;
notify(Pid, Ref, _Triggers, {error, _Seq, Reason}) ->
    ?error("Changes stream terminated abnormally due to: ~tp", [Reason]),
    Pid ! {Ref, stream_ended},
    ok.


%% @private
-spec is_observed_doc(datastore:doc(), changes_stream_processor:triggers()) -> boolean().
is_observed_doc(#document{value = Record}, Triggers) when is_tuple(Record) ->
    lists:member(element(1, Record), Triggers);
is_observed_doc(_Doc, _Triggers) ->
    false.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Send synchronous message to changes_stream_handler and await confirmation
%% that msg was received.
%% @end
%%--------------------------------------------------------------------
call_changes_stream_handler(Pid, Ref, Msg) ->
    Pid ! {Ref, Msg},
    receive
        {Ref, ok} ->
            ok
    end,
    ok.
