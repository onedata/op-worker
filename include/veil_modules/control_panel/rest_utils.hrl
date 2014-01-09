%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This hrl provides convinience functions to map and unmap records to json.
%% Because erlang records must be known at compile time, this is the only way
%% to skip the necessity of specifying record's field list in map & unmap function calls.
%% See rest_utils module.
%% @end
%% ===================================================================

-ifndef(REST_UTILS_HRL).
-define(REST_UTILS_HRL, 1).

% Compiler will insert map/1 function definition instead of this macro.
% It will allow to use rest_utils:map/2 without specifying the fields of the record.
-define(map(RecordName), 
	map(Record) ->
		rest_utils:map(Record, record_info(fields, RecordName))
).

% Compiler will insert unmap/1 function definition instead of this macro.
% It will allow to use rest_utils:unmap/3 without specifying the fields of the record.
-define(unmap(RecordName), 
	unmap(BinProplist, Record) -> 
		rest_utils:unmap(BinProplist, Record, record_info(fields, RecordName))
).

-endif.