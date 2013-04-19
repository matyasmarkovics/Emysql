%% Copyright (c) 2009-2011
%% Bill Warnecke <bill@rupture.com>,
%% Jacob Vorreuter <jacob.vorreuter@gmail.com>,
%% Henning Diedrich <hd2010@eonblast.com>,
%% Eonblast Corporation <http://www.eonblast.com>,
%% Matyas Markovics <markovics.matyas@gmail.com>
%% 
%% Permission is  hereby  granted,  free of charge,  to any person
%% obtaining  a copy of this software and associated documentation
%% files (the "Software"),to deal in the Software without restric-
%% tion,  including  without  limitation the rights to use,  copy,
%% modify, merge,  publish,  distribute,  sublicense,  and/or sell
%% copies  of the  Software,  and to  permit  persons to  whom the
%% Software  is  furnished  to do  so,  subject  to the  following
%% conditions:
%%
%% The above  copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF  MERCHANTABILITY,  FITNESS  FOR  A  PARTICULAR  PURPOSE  AND
%% NONINFRINGEMENT. IN  NO  EVENT  SHALL  THE AUTHORS OR COPYRIGHT
%% HOLDERS  BE  LIABLE FOR  ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT,  TORT  OR OTHERWISE,  ARISING
%% FROM,  OUT OF OR IN CONNECTION WITH THE SOFTWARE  OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
%% @private
-module(emysql_tcp).

-export([send_and_recv_packet/3, recv_packet/3, parse_response/4]).

-export([do_send_and_recv_packet/4, do_recv_packet/2]).
-export([
        packets/1,
        send_packet/3,
        parse/1,
        response/1,
        do_response_list/4,
        read_field_list/1,
        read_row_data/1
        ]).

-include("emysql.hrl").
-include("emysql_internal.hrl").

-type packet_result() :: #eof_packet{} | #ok_packet{} | #result_packet{} | #error_packet{}.

-spec send_and_recv_packet(port(), iodata(), integer()) -> packet_result() | [packet_result()].
send_and_recv_packet(Sock, Packet, SeqNum) ->
    case gen_tcp:send(Sock, [<<(size(Packet)):24/little, SeqNum:8>>, Packet]) of
        ok -> ok;
        {error, closed} ->
            %% If we can't communicate on the socket since it has been closed, we exit the process
            %% at this point. The exit reason is caught by `emysql:monitor_work/3` and since it is
            %% with the atom `conn_tcp_closed` we special-case that and rehandle it properly
            exit(tcp_connection_closed)
    end,
    DefaultTimeout = emysql_app:default_timeout(),
    case response_list(Sock, DefaultTimeout, ?SERVER_MORE_RESULTS_EXIST) of
        % This is a bit murky. It's compatible with former Emysql versions
        % but sometimes returns a list, e.g. for stored procedures,
        % since an extra OK package is sent at the end of their results.
        [Record] -> Record;
        List -> List
    end.

response_list(Sock, DefaultTimeout, ServerStatus) -> 
    response_list(Sock, DefaultTimeout, ServerStatus, <<>>).

response_list(_, _DefaultTimeout, 0, <<>>) -> [];  %%no further data received after last response.

response_list(Sock, DefaultTimeout, ?SERVER_MORE_RESULTS_EXIST, Buff) ->
    {Packet, Rest} = recv_packet(Sock, DefaultTimeout, Buff),
    {Response, ServerStatus, Rest2} = parse_response(Sock, DefaultTimeout, Packet, Rest),
    [ Response | response_list(Sock, DefaultTimeout, ServerStatus band ?SERVER_MORE_RESULTS_EXIST, Rest2)].

recv_packet(Sock, DefaultTimeout, Buff) ->
    {PacketLength, SeqNum, Buff2} = recv_packet_header(Sock, DefaultTimeout, Buff),
    {Data, Rest} = recv_packet_body(Sock, PacketLength, DefaultTimeout, Buff2),
    {#packet{size=PacketLength, seq_num=SeqNum, data=Data}, Rest}.

parse_response(_Sock, _Timeout, #packet{seq_num = SeqNum, data = <<0:8, Rest/binary>>}=_Packet, Buff) ->
    {AffectedRows, Rest1} = lcb(Rest),
    {InsertId, Rest2} = lcb(Rest1),
    <<ServerStatus:16/little, WarningCount:16/little, Msg/binary>> = Rest2,
    { #ok_packet{
        seq_num = SeqNum,
        affected_rows = AffectedRows,
        insert_id = InsertId,
        status = ServerStatus,
        warning_count = WarningCount,
        msg = unicode:characters_to_list(Msg) },
      ServerStatus, Buff };

% EOF: MySQL format <= 4.0, single byte. See -2-
parse_response(_Sock, _Timeout, #packet{seq_num = SeqNum, data = <<?RESP_EOF:8>>}=_Packet, Buff) ->
    { #eof_packet{
        seq_num = SeqNum },
      ?SERVER_NO_STATUS, Buff };

% EOF: MySQL format >= 4.1, with warnings and status. See -2-
parse_response(_Sock, _Timeout, #packet{seq_num = SeqNum, data = <<?RESP_EOF:8, WarningCount:16/little, ServerStatus:16/little>>}=_Packet, Buff) ->
    { #eof_packet{
        seq_num = SeqNum,
        status = ServerStatus,
        warning_count = WarningCount },
      ServerStatus, Buff };

% ERROR response: MySQL format >= 4.1. See -3-
parse_response(_Sock, _Timeout, #packet{seq_num = SeqNum, data = <<255:8, ErrNo:16/little, "#", SQLState:5/binary-unit:8, Msg/binary>>}=_Packet, Buff) ->
    { #error_packet{
        seq_num = SeqNum,
        code = ErrNo,
        sql_state = SQLState,
        msg = binary_to_list(Msg) }, % todo: test and possibly conversion to UTF-8
     ?SERVER_NO_STATUS, Buff };

% ERROR response: MySQL format <= 4.0. See -3-
parse_response(_Sock, _Timeout, #packet{seq_num = SeqNum, data = <<255:8, ErrNo:16/little, Msg/binary>>}=_Packet, Buff) ->
    { #error_packet{
        seq_num = SeqNum,
        code = ErrNo,
        sql_state = <<0>>,
        msg = binary_to_list(Msg) }, % todo: test and possibly conversion to UTF-8
     ?SERVER_NO_STATUS, Buff };

% DATA response.
parse_response(Sock, DefaultTimeout, #packet{seq_num = SeqNum, data = Data}=_Packet, Buff) ->
    {FieldCount, Rest1} = lcb(Data),
    {Extra, _} = lcb(Rest1),
    {SeqNum1, FieldList, Buff2} = recv_field_list(Sock, SeqNum+1, DefaultTimeout, Buff),
    if
        length(FieldList) =/= FieldCount ->
            exit(query_returned_incorrect_field_count);
        true ->
            ok
    end,
    {SeqNum2, Rows, ServerStatus, Buff3} = recv_row_data(Sock, FieldList, DefaultTimeout, SeqNum1+1, Buff2),
    { #result_packet{
        seq_num = SeqNum2,
        field_list = FieldList,
        rows = Rows,
        extra = Extra },
      ServerStatus, Buff3 }.

recv_packet_header(_Sock, _Timeout, <<PacketLength:24/little-integer, SeqNum:8/integer, Rest/binary>>) ->
        {PacketLength, SeqNum, Rest};
recv_packet_header(Sock, Timeout, Buff) when erlang:byte_size(Buff) < 4 ->
        case gen_tcp:recv(Sock, 0, Timeout) of
            {ok, Data} ->
                recv_packet_header(Sock, Timeout, <<Buff/binary, Data/binary>>);
            {error, Reason} ->
                exit({failed_to_recv_packet_header, Reason})
        end;
recv_packet_header(_Sock, _Timeout, Buff) ->
        exit({bad_packet_header_data, Buff}).
    
recv_packet_body(Sock, PacketLength, Timeout, Buff) ->
    case Buff of
        <<Bin:PacketLength/binary, Rest/binary>> ->
            {Bin, Rest};
        _ when erlang:byte_size(Buff) < PacketLength ->
            case gen_tcp:recv(Sock, 0, Timeout) of
                    {ok, Bin} ->
                        recv_packet_body(Sock, PacketLength , Timeout, <<Buff/binary, Bin/binary>>);
                    {error, Reason1} ->
                        exit({failed_to_recv_packet_body, Reason1})
            end
    end.

recv_field_list(Sock, SeqNum, DefaultTimeout, Buff) ->
    recv_field_list(Sock, SeqNum, DefaultTimeout,[], Buff).

recv_field_list(Sock, _SeqNum, DefaultTimeout, Acc, Buff) ->
	case recv_packet(Sock, DefaultTimeout, Buff) of
        {#packet{seq_num = SeqNum1, data = <<?RESP_EOF, _WarningCount:16/little, _ServerStatus:16/little>>}, Unparsed} ->
                        {SeqNum1, lists:reverse(Acc), Unparsed};
        {#packet{seq_num = SeqNum1, data = <<?RESP_EOF, _/binary>>}, Unparsed} ->
                        {SeqNum1, lists:reverse(Acc), Unparsed};
        {#packet{seq_num = SeqNum1, data = Data}, Unparsed} ->
			{Catalog, Rest2} = lcs(Data),
			{Db, Rest3} = lcs(Rest2),
			{Table, Rest4} = lcs(Rest3),
			{OrgTable, Rest5} = lcs(Rest4),
			{Name, Rest6} = lcs(Rest5),
			{OrgName, Rest7} = lcs(Rest6),
			<<_:1/binary, CharSetNr:16/little, Length:32/little, Rest8/binary>> = Rest7,
			<<Type:8/little, Flags:16/little, Decimals:8/little, _:2/binary, Rest9/binary>> = Rest8,
			{Default, _} = lcb(Rest9),
			Field = #field{
				seq_num = SeqNum1,
				catalog = Catalog,
				db = Db,
				table = Table,
				org_table = OrgTable,
				name = Name,
				org_name = OrgName,
				type = Type,
				default = Default,
				charset_nr = CharSetNr,
				length = Length,
				flags = Flags,
				decimals = Decimals,
 				decoder = cast_fun_for(Type)
			},
			recv_field_list(Sock, SeqNum1, DefaultTimeout, [Field|Acc], Unparsed)
	end.

send_packet(Sock, Packet, SeqNum) ->
	 io:format("~nsend_packet: SEND SeqNum: ~p, Binary: ~p~n", [SeqNum, <<(size(Packet)):24/little, SeqNum:8, Packet/binary>>]),
	 io:format("~p send_packet: send~n", [self()]),
	case gen_tcp:send(Sock, <<(size(Packet)):24/little, SeqNum:8, Packet/binary>>) of
		ok -> 
			 io:format("~p send_packet: send ok~n", [self()]),
			ok;
		{error, Reason} ->
			 io:format("~p send_packet: ERROR ~p -> EXIT~n", [self(), Reason]),
			exit({failed_to_send_packet_to_server, Reason})
	end.

%% This whole binary to record is completly needless, was just lazy to remove it from all over the place.
packets(<<PacketLength:24/little-integer, SeqNum:8/integer, Rest/binary>>) ->
    <<Data:PacketLength/binary, Rest1/binary>> = Rest,
    {#packet{ size = PacketLength, seq_num = SeqNum, data = Data }, Rest1}.

parse(Packet) ->
    parse(Packet, []).

parse(<<>>, [Acc]) -> Acc;
parse(<<>>, Acc) -> lists:reverse(Acc);
parse(BinaryPacket, Acc) when is_binary(BinaryPacket) ->
    {PacketRecord, Rest1} = packets(BinaryPacket),
    {Response, Next} = case response(PacketRecord) of
        #result_packet{} = Result ->
            {_Eof, Fields, Rest2} = parse_fields(Rest1, []),
            io:format("fields: ~p~n", [Fields]),
            {Eof, Rows, Rest3} = parse_rows(Rest2, Fields, []),
            {Result#result_packet{ field_list = Fields, rows = Rows, extra = Eof }, Rest3};
        Other ->
            {Other, Rest1}
    end,
    parse(Next, [Response|Acc]).

parse_fields(<<PacketLength:24/little-integer, SeqNum:8/integer, Rest/binary>>, Acc) ->
    <<Data:PacketLength/binary, Rest1/binary>> = Rest,
    case read_field_list(#packet{ size = PacketLength, seq_num = SeqNum, data = Data }) of
        #eof_packet{} = Eof -> {Eof, lists:reverse(Acc), Rest1};
        #field{} = Field -> parse_fields(Rest1, [Field|Acc])
    end.

parse_rows(<<PacketLength:24/little-integer, SeqNum:8/integer, Rest/binary>>, Fields, Acc) ->
    % io:format("packet_length: ~p, remaining: ~p ~n parsed so far: ~p~n", [PacketLength, size(Rest), Acc]),
    <<Data:PacketLength/binary, Rest1/binary>> = Rest,
    case read_row_data(#packet{ size = PacketLength, seq_num = SeqNum, data = Data }) of
        #eof_packet{} = Eof -> {Eof, lists:reverse(Acc), Rest1};
        RowData when is_binary(RowData) -> parse_rows(Rest1, Fields, [decode_row_data(RowData, Fields)|Acc]) 
    end.

do_send_and_recv_packet(Sock, Timeout, Packet, SeqNum) ->
        send_packet(Sock, Packet, SeqNum),
         io:format("~p send_and_recv_packet: response_list~n", [self()]),
        case do_response_list(Sock, Timeout, ?SERVER_MORE_RESULTS_EXIST, []) of
		% sometimes returns a list, e.g. for stored procedures,
		[Record] ->
			 io:format("~p send_and_recv_packet: record~n", [self()]),
			Record;
		List ->
			 io:format("~p send_and_recv_packet: list~n", [self()]),
			List
	end.

do_response_list(_, _, 0, Acc) -> lists:reverse(Acc);

do_response_list(Sock, Timeout, ?SERVER_MORE_RESULTS_EXIST, Acc) ->
        {Packet, Used} = do_recv_packet(Sock, Timeout),
	{Response, Status, TimeoutRem} =  case response(Packet) of
            #result_packet{ field_count = FC } = R -> 
                {_Eof, FieldList, Timeout1} = do_recv_field_list(Sock, Timeout-Used, lists:seq(0, FC), []),
                {Last, Rows, Timeout2} = do_recv_row_data(Sock, Timeout1, FieldList, 0),
                ResultSet = R#result_packet{ field_list = FieldList, rows = Rows, extra = Last },
                {ResultSet, Last, Timeout2};
            Else -> 
                {Else, Else, Timeout-Used}
        end,
        ServerStatus = case Status of
            #eof_packet{ status = EofStatus } -> EofStatus;
            #ok_packet{ status = OkStatus } -> OkStatus;
            #error_packet{} -> ?SERVER_NO_STATUS
        end,
        do_response_list(Sock, TimeoutRem, ServerStatus band ?SERVER_MORE_RESULTS_EXIST, [Response|Acc]).


do_recv_packet(Sock, Timeout) ->
	 io:format("~p recv_packet~n", [self()]),
	 io:format("~p recv_packet: recv_packet_header~n", [self()]),
        Start = erlang:now(),
	{PacketLength, SeqNum} = recv_packet_header(Sock, Timeout),
        Used = timer:now_diff(Start, erlang:now()) div 1000,
	 io:format("~p recv_packet: recv_packet_body~n", [self()]),
	Data = recv_packet_body(Sock, PacketLength, Timeout-Used),
	 io:format("~nrecv_packet: len: ~p, data: ~p~n", [PacketLength, Data]),
        {#packet{size=PacketLength, seq_num=SeqNum, data=Data}, timer:now_diff(Start, erlang:now()) div 1000 }.

% OK response: first byte 0. See -1-
response(#packet{seq_num = SeqNum, data = <<?RESP_OK:8, Rest/binary>>}=_Packet) ->
	 io:format("~nresponse (OK): ~p~n", [_Packet]),
	{AffectedRows, Rest1} = lcb(Rest),
	{InsertId, Rest2} = lcb(Rest1),
	<<ServerStatus:16/little, WarningCount:16/little, Rest3/binary>> = Rest2, % (*)!
        {Msg, <<>>} = lcs(Rest3),
	 io:format("- warnings: ~p~n", [WarningCount]),
	 io:format("- server status: ~p~n", [emysql_conn:hstate(ServerStatus)]),
	#ok_packet{
		seq_num = SeqNum,
		affected_rows = AffectedRows,
		insert_id = InsertId,
		status = ServerStatus,
		warning_count = WarningCount,
		msg = unicode:characters_to_list(Msg) };

% EOF: MySQL format <= 4.0, single byte. See -2-
response(#packet{seq_num = SeqNum, data = <<?RESP_EOF:8>>}=_Packet) ->
	 io:format("~nresponse (EOF v 4.0): ~p~n", [_Packet]),
	#eof_packet{ seq_num = SeqNum };

% EOF: MySQL format >= 4.1, with warnings and status. See -2-
response(#packet{seq_num = SeqNum, data = <<?RESP_EOF:8, WarningCount:16/little, ServerStatus:16/little>>}=_Packet) -> % (*)!
	 io:format("~nresponse (EOF v 4.1), Warn Count: ~p, Status ~p, Raw: ~p~n", [WarningCount, ServerStatus, _Packet]),
	 io:format("- warnings: ~p~n", [WarningCount]),
	 io:format("- server status: ~p~n", [emysql_conn:hstate(ServerStatus)]),
	#eof_packet{
		seq_num = SeqNum,
		status = ServerStatus,
		warning_count = WarningCount };

% ERROR response: MySQL format >= 4.1. See -3-
response(#packet{seq_num = SeqNum, data = <<?RESP_ERROR:8, ErrNo:16/little, "#", SQLState:5/binary-unit:8, Msg/binary>>}=_Packet) ->
	 io:format("~nresponse (Response is ERROR): SeqNum: ~p, Packet: ~p~n", [SeqNum, _Packet]),
	#error_packet{
		seq_num = SeqNum,
		code = ErrNo,
		sql_state = SQLState,
		msg = binary_to_list(Msg) }; % todo: test and possibly conversion to UTF-8

% ERROR response: MySQL format <= 4.0. See -3-
response(#packet{seq_num = SeqNum, data = <<?RESP_ERROR:8, ErrNo:16/little, Msg/binary>>}=_Packet) ->
	 io:format("~nresponse (Response is ERROR): SeqNum: ~p, Packet: ~p~n", [SeqNum, _Packet]),
	#error_packet{
		seq_num = SeqNum,
		code = ErrNo,
		msg = binary_to_list(Msg) }; % todo: test and possibly conversion to UTF-8

% DATA response.
response(#packet{seq_num = SeqNum, data = Data}=_Packet) ->
	% io:format("~nresponse (DATA): ~p~n", [_Packet]),
        {FieldCount, <<>>} = lcb(Data),
        #result_packet{
                seq_num = SeqNum,
                field_count = FieldCount }.

recv_packet_header(_Sock, Timeout) when Timeout =< 0 -> exit({recv_packet_header, etimedout});
recv_packet_header(Sock, Timeout) ->
	 io:format("~p recv_packet_header~n", [self()]),
	 io:format("~p recv_packet_header: recv~n", [self()]),
	case gen_tcp:recv(Sock, 4, Timeout) of
		{ok, <<PacketLength:24/little-integer, SeqNum:8/integer>>} ->
			 io:format("~p recv_packet_header: ok~n", [self()]),
			{PacketLength, SeqNum};
		{ok, Bin} when is_binary(Bin) ->
			 io:format("~p recv_packet_header: ERROR: exit w/bad_packet_header_data~n", [self()]),
			exit({bad_packet_header_data, Bin});
		{error, Reason} ->
			 io:format("~p recv_packet_header: ERROR: exit w/~p~n", [self(), Reason]),
			exit({failed_to_recv_packet_header, Reason})
	end.

% This was used to approach a solution for proper handling of SERVER_MORE_RESULTS_EXIST
%
% recv_packet_header_if_present(Sock) ->
%	case gen_tcp:recv(Sock, 4, 0) of
%		{ok, <<PacketLength:24/little-integer, SeqNum:8/integer>>} ->
%			{PacketLength, SeqNum};
%		{ok, Bin} when is_binary(Bin) ->
%			exit({bad_packet_header_data, Bin});
%		{error, timeout} ->
%			none;
%		{error, Reason} ->
%			exit({failed_to_recv_packet_header, Reason})
%	end.

recv_packet_body(_Sock, _, Timeout) when Timeout =< 0 -> exit({recv_packet_body, etimedout});
recv_packet_body(Sock, PacketLength, Timeout) ->
    case gen_tcp:recv(Sock, PacketLength, Timeout) of
        {ok, Bin} -> Bin;
        {error, Reason1} ->
            exit({failed_to_recv_packet_body, Reason1})
    end.

do_recv_field_list(_Sock, Timeout, [], [Eof | Acc]) -> {Eof, lists:reverse(Acc), Timeout};
do_recv_field_list(Sock, Timeout, [_|T], Acc) ->
    {Packet, Used} = do_recv_packet(Sock, Timeout),
    do_recv_field_list(Sock, Timeout-Used, T, [read_field_list(Packet)|Acc]).


read_field_list(#packet{seq_num = SeqNum1, data = <<?RESP_EOF, WarningCount:16/little, ServerStatus:16/little>>}) -> % (*)!
     io:format("- eof: ~p~n", [emysql_conn:hstate(ServerStatus)]),
    #eof_packet{
	seq_num = SeqNum1,
	status = ServerStatus,
	warning_count = WarningCount };
read_field_list(#packet{seq_num = SeqNum1, data = <<?RESP_EOF, _/binary>>}) ->
     io:format("- eof~n", []),
    #eof_packet{
	seq_num = SeqNum1 };
read_field_list(#packet{seq_num = SeqNum1, data = Data}) ->
    {Catalog, Rest2} = lcs(Data),
    {Db, Rest3} = lcs(Rest2),
    {Table, Rest4} = lcs(Rest3),
    {OrgTable, Rest5} = lcs(Rest4),
    {Name, Rest6} = lcs(Rest5),
    {OrgName, Rest7} = lcs(Rest6),
    {_FixedLength, Rest8} = lcb(Rest7),
    <<CharSetNr:16/little, Length:32/little, Rest9/binary>> = Rest8,
    <<Type:8/little, Flags:16/little, Decimals:8/little, _:2/binary, Rest10/binary>> = Rest9,
    {Default, <<>>} = lcs(Rest10),
    #field{
            seq_num = SeqNum1,
            catalog = Catalog,
            db = Db,
            table = Table,
            org_table = OrgTable,
            name = Name,
            org_name = OrgName,
            type = Type,
            default = Default, 
            charset_nr = CharSetNr,
            length = Length,
            flags = Flags,
            decimals = Decimals
            }.

do_recv_row_data(Sock, Timeout, FieldList, SeqNum) ->
    {Packet, Used} = do_recv_packet(Sock, Timeout),
    do_recv_row_data(Sock, Timeout-Used, FieldList, SeqNum, read_row_data(Packet), []).

do_recv_row_data(_Sock, Timeout, _FieldList, _SeqNum, #eof_packet{} = Eof, Acc) -> {Eof, lists:reverse(Acc), Timeout};
do_recv_row_data(Sock, Timeout, FieldList, SeqNum, RowData, Acc) ->
    {Packet, Used} = do_recv_packet(Sock, Timeout),
    do_recv_row_data(Sock, Timeout-Used, FieldList, SeqNum+1, read_row_data(Packet), [decode_row_data(RowData, FieldList)|Acc]).

read_row_data(#packet{seq_num = SeqNum1, data = <<?RESP_EOF, WarningCount:16/little, ServerStatus:16/little>>}) ->
    io:format("- eof: ~p~n", [emysql_conn:hstate(ServerStatus)]),
    #eof_packet{
        seq_num = SeqNum1,
        status = ServerStatus,
        warning_count = WarningCount };
read_row_data(#packet{seq_num = SeqNum1, data = <<?RESP_EOF, _/binary>>}) ->
    io:format("- eof.~n", []),
    #eof_packet{
        seq_num = SeqNum1};
% ERROR response: MySQL format >= 4.1. See -3-
read_row_data(#packet{seq_num = SeqNum, data = <<?RESP_ERROR:8, ErrNo:16/little, "#", SQLState:5/binary-unit:8, Msg/binary>>}=_Packet) ->
    io:format("~nresponse (Response is ERROR): SeqNum: ~p, Packet: ~p~n", [SeqNum, _Packet]),
    #error_packet{
        seq_num = SeqNum,
        code = ErrNo,
        sql_state = SQLState,
        msg = binary_to_list(Msg) }; % todo: test and possibly conversion to UTF-8
% ERROR response: MySQL format <= 4.0. See -3-
read_row_data(#packet{seq_num = SeqNum, data = <<?RESP_ERROR:8, ErrNo:16/little, Msg/binary>>}=_Packet) ->
    io:format("~nresponse (Response is ERROR): SeqNum: ~p, Packet: ~p~n", [SeqNum, _Packet]),
    #error_packet{
        seq_num = SeqNum,
        code = ErrNo,
        msg = binary_to_list(Msg) }; % todo: test and possibly conversion to UTF-8
read_row_data(#packet{seq_num = _SeqNum1, data = RowData}) ->
    % io:format("Seq: ~p raw: ~p~n", [SeqNum1, RowData]),
    RowData.


recv_row_data(Socket, FieldList, DefaultTimeout, SeqNum, Buff) ->
    recv_row_data(Socket, FieldList, DefaultTimeout, SeqNum, Buff, []).

recv_row_data(Socket, FieldList, Timeout, SeqNum, Buff, Acc) ->
       case parse_buffer(FieldList,Buff, Acc) of
                {ok, NotParsed, NewAcc, Missing} ->
                    case gen_tcp:recv(Socket, Missing, Timeout) of
                        {ok, Data} ->
                            recv_row_data(Socket, FieldList, Timeout, SeqNum+1,  <<NotParsed/binary, Data/binary>>, NewAcc);
                        {error, Reason} ->
                            exit({failed_to_recv_row, Reason})
                    end;
                {eof, Seq, NewAcc, ServerStatus, NotParsed} ->
                    {Seq, lists:reverse(NewAcc), ServerStatus, NotParsed}
        end.

parse_buffer(FieldList,<<PacketLength:24/little-integer, SeqNum:8/integer, PacketData:PacketLength/binary, Rest/binary>>, Acc) ->
    case PacketData of
        <<?RESP_EOF, _WarningCount:16/little, ServerStatus:16/little>> ->
            {eof, SeqNum, Acc, ServerStatus, Rest};
        <<?RESP_EOF, _/binary>> ->
            {eof, SeqNum, Acc, ?SERVER_NO_STATUS, Rest};
        _ ->
            Row = decode_row_data(PacketData, FieldList),
            parse_buffer(FieldList,Rest, [Row|Acc])
    end;
parse_buffer(_FieldList, Buff = <<PacketLength:24/little-integer, _SeqNum:8/integer, PacketData/binary>>, Acc) ->
    Missing = PacketLength - size(PacketData),
    if
        Missing =< ?TCP_RECV_BUFFER ->
            {ok, Buff, Acc, 0};
        true ->
            {ok, Buff, Acc, Missing}
    end;
parse_buffer(_FieldList,Buff, Acc) ->
    {ok, Buff, Acc, 0}.

decode_row_data(<<>>, []) ->
    [];
decode_row_data(<<Length:8, Data:Length/binary, Tail/binary>>, [Field|Rest]) 
        when Length =< 250 ->
    [type_cast_row_data(Data, Field) | decode_row_data(Tail, Rest)];
%% 251 means null
decode_row_data(<<251:8, Tail/binary>>, [Field|Rest]) ->  
    [type_cast_row_data(undefined, Field) | decode_row_data(Tail, Rest)];
decode_row_data(<<252:8, Length:16/little, Data:Length/binary, Tail/binary>>, [Field|Rest]) ->
    [type_cast_row_data(Data, Field) | decode_row_data(Tail, Rest)];
decode_row_data(<<253:8, Length:24/little, Data:Length/binary, Tail/binary>>, [Field|Rest]) ->
    [type_cast_row_data(Data, Field) | decode_row_data(Tail, Rest)];
decode_row_data(<<254:8, Length:64/little, Data:Length/binary, Tail/binary>>, [Field|Rest]) ->
    [type_cast_row_data(Data, Field) | decode_row_data(Tail, Rest)].

cast_fun_for(Type) ->
    Map = [{?FIELD_TYPE_VARCHAR, fun identity/1},
           {?FIELD_TYPE_TINY_BLOB, fun identity/1},
           {?FIELD_TYPE_MEDIUM_BLOB, fun identity/1},
           {?FIELD_TYPE_LONG_BLOB, fun identity/1},
           {?FIELD_TYPE_BLOB, fun identity/1},
           {?FIELD_TYPE_VAR_STRING, fun identity/1},
           {?FIELD_TYPE_STRING, fun identity/1},
           {?FIELD_TYPE_TINY, fun to_integer/1},
           {?FIELD_TYPE_SHORT, fun to_integer/1},
           {?FIELD_TYPE_LONG, fun to_integer/1},
           {?FIELD_TYPE_LONGLONG, fun to_integer/1},
           {?FIELD_TYPE_INT24, fun to_integer/1},
           {?FIELD_TYPE_YEAR, fun to_integer/1},
           {?FIELD_TYPE_DECIMAL, fun to_float/1},
           {?FIELD_TYPE_NEWDECIMAL, fun to_float/1},
           {?FIELD_TYPE_FLOAT, fun to_float/1},
           {?FIELD_TYPE_DOUBLE, fun to_float/1},
           {?FIELD_TYPE_DATE, fun to_date/1},
           {?FIELD_TYPE_TIME, fun to_time/1},
           {?FIELD_TYPE_TIMESTAMP, fun to_timestamp/1},
           {?FIELD_TYPE_DATETIME, fun to_timestamp/1},
           {?FIELD_TYPE_BIT, fun to_bit/1}],
            % TODO:
            % ?FIELD_TYPE_NEWDATE
            % ?FIELD_TYPE_ENUM
            % ?FIELD_TYPE_SET
            % ?FIELD_TYPE_GEOMETRY
    case lists:keyfind(Type, 1, Map) of
        false ->
            fun identity/1;
        {Type, F} ->
            F
    end.

identity(Data) -> Data.

to_integer(Data) -> list_to_integer(binary_to_list(Data)).

to_float(Data) ->
    {ok, [Num], _Leftovers} = case io_lib:fread("~f", binary_to_list(Data)) of
                                           % note: does not need conversion
        {error, _} ->
          case io_lib:fread("~d", binary_to_list(Data)) of  % note: does not need conversion
            {ok, [_], []} = Res ->
              Res;
            {ok, [X], E} ->
              io_lib:fread("~f", lists:flatten(io_lib:format("~w~s~s" ,[X,".0",E])))
          end
        ;
        Res ->
          Res
    end,
    Num.

to_date(Data) ->
    case io_lib:fread("~d-~d-~d", binary_to_list(Data)) of  % note: does not need conversion
        {ok, [Year, Month, Day], _} ->
            {date, {Year, Month, Day}};
        {error, _} ->
            binary_to_list(Data);  % todo: test and possibly conversion to UTF-8
        _ ->
            exit({error, bad_date})
    end.

to_time(Data) ->
    case io_lib:fread("~d:~d:~d", binary_to_list(Data)) of  % note: does not need conversion
        {ok, [Hour, Minute, Second], _} ->
            {time, {Hour, Minute, Second}};
        {error, _} ->
            binary_to_list(Data);  % todo: test and possibly conversion to UTF-8
        _ ->
            exit({error, bad_time})
    end.

to_timestamp(Data) ->
    case io_lib:fread("~d-~d-~d ~d:~d:~d", binary_to_list(Data)) of % note: does not need conversion
        {ok, [Year, Month, Day, Hour, Minute, Second], _} ->
            {datetime, {{Year, Month, Day}, {Hour, Minute, Second}}};
        {error, _} ->
            binary_to_list(Data);   % todo: test and possibly conversion to UTF-8
        _ ->
            exit({error, datetime})
    end.

to_bit(<<1>>) -> 1;
to_bit(<<0>>) -> 0.

type_cast_row_data(undefined, _) -> undefined;
type_cast_row_data(Data, #field{decoder = F}) -> F(Data).

%% lcb/1 decodes length-coded-integer values
lcb(<<>>) -> {<<>>, <<>>}; % This clause should be removed when we have control
lcb(<< Value:8, Rest/bits >>) when Value =< 250 -> {Value, Rest};
lcb(<< 252:8, Value:16/little, Rest/bits >>) -> {Value, Rest};
lcb(<< 253:8, Value:24/little, Rest/bits >>) -> {Value, Rest};
lcb(<< 254:8, Value:64/little, Rest/bits >>) -> {Value, Rest}.

%% lcs/1 decodes length-encoded-string values
lcs(<< 251:8, Rest/bits >>) -> {undefined, Rest};
lcs(Bin) ->
    {Length, Rest} = lcb(Bin),
    << String:Length/binary, Excess/binary>> = Rest,
    {String, Excess}.
