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

-include("http/rest/cdmi/cdmi_capabilities.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

%% API
-export([prepare/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Prepares map formatted answer with field names from given list of binaries
%%--------------------------------------------------------------------
-spec prepare([FieldName :: binary()], maps:map()) -> maps:map().
prepare([], _State) ->
    #{};
prepare([<<"objectType">> | Tail], State) ->
    (prepare(Tail, State))#{<<"objectType">> => <<"application/cdmi-object">>};
prepare([<<"objectID">> | Tail], #{guid := Guid} = State) ->
    {ok, Id} = cdmi_id:guid_to_objectid(Guid),
    (prepare(Tail, State))#{<<"objectID">> => Id};
prepare([<<"objectName">> | Tail], #{path := Path} = State) ->
    (prepare(Tail, State))#{<<"objectName">> => filename:basename(Path)};
prepare([<<"parentURI">> | Tail], #{path := <<"/">>} = State) ->
    (prepare(Tail, State))#{<<"parentURI">> => <<>>};
prepare([<<"parentURI">> | Tail], #{path := Path} = State) ->
    ParentURI = filepath_utils:parent_dir(Path),
    (prepare(Tail, State))#{<<"parentURI">> => ParentURI};
prepare([<<"parentID">> | Tail], #{path := <<"/">>} = State) ->
    (prepare(Tail, State))#{<<"parentID">> => <<>>};
prepare([<<"parentID">> | Tail], #{path := Path, auth := Auth} = State) ->
    {ok, #file_attr{guid = Guid}} =
        onedata_file_api:stat(Auth, {path, filename:dirname(Path)}),
    {ok, Id} = cdmi_id:guid_to_objectid(Guid),
    (prepare(Tail, State))#{<<"parentID">> => Id};
prepare([<<"capabilitiesURI">> | Tail], State) ->
    (prepare(Tail, State))#{<<"capabilitiesURI">> => ?dataobject_capability_path};
prepare([<<"completionStatus">> | Tail], #{auth := Auth, guid := Guid} = State) ->
    CompletionStatus = cdmi_metadata:get_cdmi_completion_status(Auth, {guid, Guid}),
    (prepare(Tail, State))#{<<"completionStatus">> => CompletionStatus};
prepare([<<"mimetype">> | Tail], #{auth := Auth, guid := Guid} = State) ->
    Mimetype = cdmi_metadata:get_mimetype(Auth, {guid, Guid}),
    (prepare(Tail, State))#{<<"mimetype">> => Mimetype};
prepare([<<"metadata">> | Tail], #{auth := Auth, attributes := Attrs = #file_attr{guid = Guid}} = State) ->
    (prepare(Tail, State))#{<<"metadata">> => cdmi_metadata:prepare_metadata(Auth, {guid, Guid}, <<>>, Attrs)};
prepare([{<<"metadata">>, Prefix} | Tail], #{auth := Auth, attributes := Attrs = #file_attr{guid = Guid}} = State) ->
    (prepare(Tail, State))#{<<"metadata">> => cdmi_metadata:prepare_metadata(Auth, {guid, Guid}, Prefix, Attrs)};
prepare([<<"metadata">> | Tail], #{auth := Auth, guid := Guid} = State) ->
    (prepare(Tail, State))#{<<"metadata">> => cdmi_metadata:prepare_metadata(Auth, {guid, Guid})};
prepare([{<<"metadata">>, Prefix} | Tail], #{auth := Auth, guid := Guid} = State) ->
    (prepare(Tail, State))#{<<"metadata">> => cdmi_metadata:prepare_metadata(Auth, {guid, Guid}, Prefix)};
prepare([<<"valuetransferencoding">> | Tail], #{auth := Auth, guid := Guid} = State) ->
    Encoding = cdmi_metadata:get_encoding(Auth, {guid, Guid}),
    (prepare(Tail, State))#{<<"valuetransferencoding">> => Encoding};
prepare([<<"value">> | Tail], State) ->
    (prepare(Tail, State))#{<<"value">> => {range, default}};
prepare([{<<"value">>, From, To} | Tail], State) ->
    (prepare(Tail, State))#{<<"value">> => {range, {From, To}}};
prepare([<<"valuerange">> | Tail], #{options := Opts, attributes := Attrs} = State) ->
    case lists:keyfind(<<"value">>, 1, Opts) of
        {<<"value">>, From, To} ->
            (prepare(Tail, State))#{<<"valuerange">> => iolist_to_binary([integer_to_binary(From), <<"-">>, integer_to_binary(To)])};
        _ ->
            (prepare(Tail, State))#{<<"valuerange">> => iolist_to_binary([<<"0-">>, integer_to_binary(Attrs#file_attr.size - 1)])} %todo fix 0--1 when file is empty
    end;
prepare([_Other | Tail], State) ->
    prepare(Tail, State).

%%%===================================================================
%%% Internal functions
%%%===================================================================