%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Prepare answer for cdmi object request.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_object_answer).
-author("Tomasz Lichon").

-include("modules/http_worker/rest/cdmi/cdmi_capabilities.hrl").
-include("modules/http_worker/rest/cdmi/cdmi_errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

%% API
-export([prepare/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Prepares proplist formatted answer with field names from given list of binaries
%%--------------------------------------------------------------------
-spec prepare([FieldName :: binary()], #{}) ->
    [{FieldName :: binary(), Value :: term()}].
prepare([], _State) ->
    [];
prepare([<<"objectType">> | Tail], State) ->
    [{<<"objectType">>, <<"application/cdmi-object">>} | prepare(Tail, State)];
%% prepare([<<"objectID">> | Tail], #{attributes := #file_attr{uuid = Uuid}} = State) -> todo introduce objectid
%%     [{<<"objectID">>, cdmi_id:uuid_to_objectid(Uuid)} | prepare(Tail, State)];
prepare([<<"objectName">> | Tail], #{path := Path} = State) ->
    [{<<"objectName">>, filename:basename(Path)} | prepare(Tail, State)];
prepare([<<"parentURI">> | Tail], #{path := <<"/">>} = State) ->
    [{<<"parentURI">>, <<>>} | prepare(Tail, State)];
prepare([<<"parentURI">> | Tail], #{path := Path} = State) ->
    ParentURI = str_utils:ensure_ends_with_slash(filename:dirname(Path)),
    [{<<"parentURI">>, ParentURI} | prepare(Tail, State)];
%% prepare([<<"parentID">> | Tail], #{path := <<"/">>} = State) -> todo introduce objectid
%%     [{<<"parentID">>, <<>>} | prepare(Tail, State)];
%% prepare([<<"parentID">> | Tail], #{path := Path, auth := Auth} = State) ->
%%     {ok, #file_attr{uuid = Uuid}} =
%%         onedata_file_api:stat(Auth, {path, filename:dirname(Path)}),
%%     [{<<"parentID">>, cdmi_id:uuid_to_objectid(Uuid)} | prepare(Tail, State)];
prepare([<<"capabilitiesURI">> | Tail], State) ->
    [{<<"capabilitiesURI">>, ?dataobject_capability_path} | prepare(Tail, State)];
prepare([<<"completionStatus">> | Tail], #{auth := Auth, attributes := #file_attr{uuid = Uuid}} = State) ->
    CompletionStatus = cdmi_metadata:get_completion_status(Auth, {uuid, Uuid}),
    [{<<"completionStatus">>, CompletionStatus} | prepare(Tail, State)];
prepare([<<"mimetype">> | Tail], #{auth := Auth, attributes := #file_attr{uuid = Uuid}} = State) ->
    Mimetype = cdmi_metadata:get_mimetype(Auth, {uuid, Uuid}),
    [{<<"mimetype">>, Mimetype} | prepare(Tail, State)];
prepare([<<"metadata">> | Tail], #{auth := Auth, attributes := Attrs = #file_attr{uuid = Uuid}} = State) ->
    [{<<"metadata">>, cdmi_metadata:prepare_metadata(Auth, {uuid, Uuid}, Attrs)} | prepare(Tail, State)];
prepare([{<<"metadata">>, Prefix} | Tail], #{auth := Auth, attributes := Attrs = #file_attr{uuid = Uuid}} = State) ->
    [{<<"metadata">>, cdmi_metadata:prepare_metadata(Auth, {uuid, Uuid}, Prefix, Attrs)} | prepare(Tail, State)];
prepare([<<"valuetransferencoding">> | Tail], #{auth := Auth, attributes := #file_attr{uuid = Uuid}} = State) ->
    Encoding = cdmi_metadata:get_encoding(Auth, {uuid, Uuid}),
    [{<<"valuetransferencoding">>, Encoding} | prepare(Tail, State)];
prepare([<<"value">> | Tail], State) ->
    [{<<"value">>, {range, default}} | prepare(Tail, State)];
prepare([{<<"value">>, From, To} | Tail], State) ->
    [{<<"value">>, {range, {From, To}}} | prepare(Tail, State)];
prepare([<<"valuerange">> | Tail], #{options := Opts, attributes := Attrs} = State) ->
    case lists:keyfind(<<"value">>, 1, Opts) of
        {<<"value">>, From, To} ->
            [{<<"valuerange">>, iolist_to_binary([integer_to_binary(From), <<"-">>, integer_to_binary(To)])} | prepare(Tail, State)];
        _ ->
            [{<<"valuerange">>, iolist_to_binary([<<"0-">>, integer_to_binary(Attrs#file_attr.size - 1)])} | prepare(Tail, State)]
    end;
prepare([_Other | Tail], State) ->
    prepare(Tail, State).

%%%===================================================================
%%% Internal functions
%%%===================================================================