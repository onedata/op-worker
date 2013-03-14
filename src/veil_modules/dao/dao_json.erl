%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: BigCouch document management module
%% @end
%% ===================================================================
-module(dao_json).

-include_lib("veil_modules/dao/couch_db.hrl").

-import(dao_helper, [name/1]).

-type json_object() :: {json_field_list()}.
-type json_field_list() :: [json_field()].
-type json_field() :: {Name :: binary(), Value :: any()}.

%% API
-export([mk_doc/1, mk_str/1, mk_obj/0, mk_bin/1, mk_field/3, mk_fields/3, rm_field/2, rm_fields/2, get_field/2]).

%% ===================================================================
%% API functions
%% ===================================================================

%% mk_doc/1
%% ====================================================================
%% @doc Returns new BigCouch document with given Id
-spec mk_doc(Id :: string()) -> #doc{}.
%% ====================================================================
mk_doc(Id) ->
    #doc{id = name(Id)}.

%% mk_str/1
%% ====================================================================
%% @doc Converts given string to binary form used by BigCouch/CouchDB
-spec mk_str(Str :: string()) -> binary().
%% ====================================================================
mk_str(Str) ->
    name(Str).

%% mk_obj/0
%% ====================================================================
%% @doc Returns empty json object structure used by BigCouch/CouchDB
-spec mk_obj() -> {[]}.
%% ====================================================================
mk_obj() ->
    {[]}.

%% mk_bin/1
%% ====================================================================
%% @doc Converts given term to binary form used by BigCouch/CouchDB
-spec mk_bin(Term :: term()) -> binary().
%% ====================================================================
mk_bin(Term) ->
    term_to_binary(Term).


%% mk_field/3
%% ====================================================================
%% @doc Inserts new field into given document or JSON object
-spec mk_field(DocOrObj, Name :: string(), Value :: term()) -> DocOrObj when
      DocOrObj :: #doc{} | json_object().
%% ====================================================================
mk_field(Doc = #doc{body = Body}, Name, Value) ->
    Doc#doc{body = mk_field(Body, Name, Value)};
mk_field({Fields}, Name, Value) ->
    {NewFields} = rm_field({Fields}, Name),
    {[{name(Name), Value} | NewFields]}.


%% mk_fields/3
%% ====================================================================
%% @doc Inserts new fields into given document or JSON object
-spec mk_fields(DocOrObj, [Names :: string()], [Values :: term()]) -> DocOrObj when
      DocOrObj :: #doc{} | json_object().
%% ====================================================================
mk_fields(Doc = #doc{body = Body}, Names, Values) when is_list(Names), is_list(Values) ->
    Doc#doc{body = mk_fields(Body, Names, Values)};
mk_fields({Fields}, Names, Values) when is_list(Names), is_list(Values) ->
    {NewFields} = rm_fields({Fields}, Names),
    {[{name(X), Y} || {X, Y} <- lists:zip(Names, Values)] ++ NewFields}.


%% rm_field/2
%% ====================================================================
%% @doc Removes field from given document or JSON object
-spec rm_field(DocOrObj, Name :: string()) -> DocOrObj when
      DocOrObj :: #doc{} | json_object().
%% ====================================================================
rm_field(Doc = #doc{body = Body}, Name) ->
    Doc#doc{body = rm_field(Body, Name)};
rm_field({Fields}, Name) ->
    {[X || {FName, _} = X <- Fields, name(FName) =/= name(Name)]}.


%% rm_fields/2
%% ====================================================================
%% @doc Removes fields from given document or JSON object
-spec rm_fields(DocOrObj, [Name :: string()]) -> DocOrObj when
      DocOrObj :: #doc{} | json_object().
%% ====================================================================
rm_fields(Doc = #doc{body = Body}, Names) when is_list(Names) ->
    Doc#doc{body = rm_fields(Body, Names)};
rm_fields({Fields}, Names) when is_list(Names) ->
    Names1 = [name(X) || X <- Names],
    {[X || {FName, _} = X <- Fields, not lists:member(name(FName), Names1)]}.


%% get_field/2
%% ====================================================================
%% @doc Returns field's value from given document or JSON object
-spec get_field(DocOrObj, Name :: string()) -> any() | {error, not_found} | {error, invalid_object} when
      DocOrObj :: #doc{} | json_object().
%% ====================================================================
get_field(_Doc = #doc{body = Body}, Name) ->
    get_field(Body, Name);
get_field({Fields}, Name) ->
    case [Value || {X, Value} <- Fields, name(X) =:= name(Name)] of
        [] -> {error, not_found};
        [V] -> V;
        _ -> {error, invalid_object}
    end.


%% ===================================================================
%% Internal functions
%% ===================================================================

