%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Functions that select user defined handlers registered in
%%% content_types_provided and content_types_accepted functions.
%%% @end
%%%--------------------------------------------------------------------
-module(plugin_callback_selector).
-author("Tomasz Lichon").

-type content_type_callback() ::
{binary() | {Type :: binary(), SubType :: binary(), content_type_params()}, module()}.

-type content_type_params() :: '*' | [{binary(), binary()}].

-export_type([content_type_callback/0, content_type_params/0]).

-include("http/rest/http_status.hrl").

%% API
-export([select_accept_callback/1, select_provide_callback/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Select callback that accepts incoming resource
%% @end
%%--------------------------------------------------------------------
-spec select_accept_callback(cowboy_req:req()) -> {ok, {cowboy_req:req(), atom()}}.
select_accept_callback(Req) ->
    ContentTypesAccepted = request_context:get_content_types_accepted(),
    NormalizedContentTypesAccepted = normalize_content_types(ContentTypesAccepted),
    ContentType = cowboy_req:parse_header(<<"content-type">>, Req),
    {ok, {Req, choose_accept_content_type_callback(ContentType, NormalizedContentTypesAccepted)}}.

%%--------------------------------------------------------------------
%% @doc
%% Select callback that provides resource.
%% @end
%%--------------------------------------------------------------------
-spec select_provide_callback(cowboy_req:req()) -> {ok, {cowboy_req:req(), atom()}}.
select_provide_callback(Req) ->
    ContentTypesProvided = request_context:get_content_types_provided(),
    NormalizedContentTypesProvided = normalize_content_types(ContentTypesProvided),
    case cowboy_req:parse_header(<<"accept">>, Req) of
        undefined ->
            [{_, Fun} | _] = NormalizedContentTypesProvided,
            {ok, {Req, Fun}};
        Accepts ->
            PrioritizedAccepts = prioritize_accept(Accepts),
            {ok,
                {Req,
                    choose_provide_content_type_callback(Req, NormalizedContentTypesProvided, PrioritizedAccepts)
            }}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Normalizes content_type_callback() list to form
%% {Type :: binary(), SubType :: binary(), content_type_params()}
%% @end
%%--------------------------------------------------------------------
-spec normalize_content_types([content_type_callback()]) -> [content_type_callback()].
normalize_content_types([]) ->
    [];
normalize_content_types([{ContentType, Callback} | Rest]) when is_binary(ContentType) ->
    [{cow_http_hd:parse_content_type(ContentType), Callback} | normalize_content_types(Rest)];
normalize_content_types([Normalized | Rest]) ->
    [Normalized | normalize_content_types(Rest)].

%%--------------------------------------------------------------------
%% @doc
%% Chooses callback that can handle given content type.
%% @end
%%--------------------------------------------------------------------
-spec choose_accept_content_type_callback(binary(), [content_type_callback()]) -> atom().
choose_accept_content_type_callback(ContentType, [{Accepted, Fun} | _Tail])
    when Accepted =:= '*'; Accepted =:= ContentType ->
    Fun;
choose_accept_content_type_callback({Type, SubType, Param}, [{{Type, SubType, AcceptedParam}, Fun} | _Tail])
    when AcceptedParam =:= '*'; AcceptedParam =:= Param ->
    Fun;
choose_accept_content_type_callback(ContentType, [_ | Tail]) ->
    choose_accept_content_type_callback(ContentType, Tail).

%%--------------------------------------------------------------------
%% @doc
%% Chooses callback that can handle given content type.
%% @end
%%--------------------------------------------------------------------
-spec choose_provide_content_type_callback(cowboy_req:req(), list(), list()) -> module().
choose_provide_content_type_callback(Req, CTP, [MediaType | Tail]) ->
    match_media_type(Req, CTP, Tail, MediaType).

%%--------------------------------------------------------------------
%% @doc Sorts list of accepted content types by their quality.
%%--------------------------------------------------------------------
-spec prioritize_accept(list()) -> list().
prioritize_accept(Accept) ->
    lists:sort(
        fun({MediaTypeA, _Quality, _AcceptParamsA}, {MediaTypeB, _Quality, _AcceptParamsB}) ->
            %% Same quality, check precedence in more details.
            prioritize_mediatype(MediaTypeA, MediaTypeB);
            ({_MediaTypeA, QualityA, _AcceptParamsA},
                {_MediaTypeB, QualityB, _AcceptParamsB}) ->
                %% Just compare the quality.
                QualityA > QualityB
        end, Accept).

%%--------------------------------------------------------------------
%% @doc
%% Media ranges can be overridden by more specific media ranges or
%% specific media types. If more than one media range applies to a given
%% type, the most specific reference has precedence.
%%
%% We always choose B over A when we can't decide between the two.
%%--------------------------------------------------------------------
-spec prioritize_mediatype(tuple(), tuple()) -> boolean().
prioritize_mediatype({TypeA, SubTypeA, ParamsA}, {TypeB, SubTypeB, ParamsB}) ->
    case TypeB of
        TypeA ->
            case SubTypeB of
                SubTypeA -> length(ParamsA) > length(ParamsB);
                <<"*">> -> true;
                _Any -> false
            end;
        <<"*">> -> true;
        _Any -> false
    end.

%%--------------------------------------------------------------------
%% @doc
%% Matches accepted media types with provided contents and returns
%% adequate handler
%% @end
%%--------------------------------------------------------------------
-spec match_media_type(cowboy_req:req(), list(), list(), tuple()) -> module().
match_media_type(Req, CTP, AcceptTail, MediaType = {{<<"*">>, <<"*">>, _Params_A}, _QA, _APA}) ->
    match_media_type_params(Req, CTP, AcceptTail, MediaType);
match_media_type(Req, CTP = [{{Type, SubType_P, _PP}, _Fun} | _Tail], AcceptTail,
    MediaType = {{Type, SubType_A, _PA}, _QA, _APA})
    when SubType_P =:= SubType_A; SubType_A =:= <<"*">> ->
    match_media_type_params(Req, CTP, AcceptTail, MediaType);
match_media_type(Req, [_Any | CTPTail], AcceptTail, MediaType) ->
    match_media_type(Req, CTPTail, AcceptTail, MediaType).

%%--------------------------------------------------------------------
%% @doc
%% Matches parameters of accepted media types with provided contents and returns
%% adequate handler
%% @end
%%--------------------------------------------------------------------
-spec match_media_type_params(cowboy_req:req(), list(), list(), tuple()) -> module().
match_media_type_params(_Req, [{{_TP, _STP, '*'}, Callback} | _], _, _) ->
    Callback;
match_media_type_params(Req, [{{_TP, _STP, Params_P}, Callback} | Tail],
    AcceptTail, MediaType = {{_TA, _STA, Params_A}, _QA, _APA}) ->
    case lists:sort(Params_P) =:= lists:sort(Params_A) of
        true ->
            Callback;
        false ->
            match_media_type(Req, AcceptTail, Tail, MediaType)
    end.
