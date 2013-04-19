%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module is able to do additional translation of record
%% decoded using protocol buffer e.g. it can change record "atom" to
%% Erlang atom type.
%% @end
%% ===================================================================

-module(records_translator).
-include("communication_protocol_pb.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([translate/1, translate_to_record/1]).

%% ====================================================================
%% API functions
%% ====================================================================

%% translate/1
%% ====================================================================
%% @doc Translates record to simpler terms if possible.
-spec translate(Record :: record()) -> Result when
  Result ::  term().
%% ====================================================================
translate(Record) when is_record(Record, atom) ->
  list_to_atom(Record#atom.value);

translate(Record) ->
  Record.

%% translate_to_record/1
%% ====================================================================
%% @doc Translates term to record if possible.
-spec translate_to_record(Value :: term()) -> Result when
  Result ::  record() | term().
%% ====================================================================
translate_to_record(Value) when is_atom(Value) ->
  #atom{value = atom_to_list(Value)};

translate_to_record(Value) ->
  Value.