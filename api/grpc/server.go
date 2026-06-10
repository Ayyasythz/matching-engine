package grpc

import (
	"context"
	"sync"

	"github.com/Ayyasythz/matching-engine/api/grpc/pb/github.com/Ayyasythz/matching-engine/api/grpc/pb"
	"github.com/Ayyasythz/matching-engine/engine"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	pb.UnimplementedExchangeServer

	eng    *engine.Engine
	events <-chan engine.Event

	mu          sync.RWMutex
	subscribers []chan engine.Event
}

func NewServer(eng *engine.Engine, events <-chan engine.Event) *Server {
	s := &Server{
		eng:    eng,
		events: events,
	}
	go s.fanOut()
	return s
}

func (s *Server) fanOut() {
	for ev := range s.events {
		s.mu.RLock()
		for _, sub := range s.subscribers {
			select {
			case sub <- ev:
			default: // slow subscriber: drop rather than block the fan-out
			}
		}
		s.mu.RUnlock()
	}
}

func (s *Server) subscribe() chan engine.Event {
	ch := make(chan engine.Event, 256)
	s.mu.Lock()
	s.subscribers = append(s.subscribers, ch)
	s.mu.Unlock()
	return ch
}

func (s *Server) unsubscribe(ch chan engine.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, sub := range s.subscribers {
		if sub == ch {
			s.subscribers = append(s.subscribers[:i], s.subscribers[i+1:]...)
			return
		}
	}
}

func (s *Server) SubmitOrder(_ context.Context, req *pb.SubmitOrderRequest) (*pb.SubmitOrderResponse, error) {
	price, err := decimal.NewFromString(req.Price)
	if err != nil && req.Price != "" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid price: %v", err)
	}
	qty, err := decimal.NewFromString(req.Qty)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid qty: %v", err)
	}

	o := engine.NewOrder(
		req.Id,
		req.Symbol,
		engine.Side(req.Side),
		engine.OrderType(req.Type),
		price,
		qty,
	)

	if err := s.eng.Submit(o); err != nil {
		return &pb.SubmitOrderResponse{OrderId: o.ID, Error: err.Error()}, nil
	}
	return &pb.SubmitOrderResponse{OrderId: o.ID, Status: string(o.Status)}, nil
}

func (s *Server) CancelOrder(_ context.Context, req *pb.CancelOrderRequest) (*pb.CancelOrderResponse, error) {
	if err := s.eng.Cancel(req.OrderId); err != nil {
		return &pb.CancelOrderResponse{Success: false, Error: err.Error()}, nil
	}
	return &pb.CancelOrderResponse{Success: true}, nil
}

func (s *Server) StreamEvents(req *pb.StreamEventsRequest, stream pb.Exchange_StreamEventsServer) error {
	sub := s.subscribe()
	defer s.unsubscribe(sub)

	for {
		select {
		case ev := <-sub:
			if req.Symbol != "" && ev.Symbol != req.Symbol {
				continue
			}
			if err := stream.Send(eventToProto(ev)); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

func eventToProto(ev engine.Event) *pb.EventResponse {
	return &pb.EventResponse{
		Type:      string(ev.Type),
		Seq:       ev.Seq,
		Timestamp: timestamppb.New(ev.Timestamp),
		OrderId:   ev.OrderID,
		Symbol:    ev.Symbol,
		Price:     ev.Price.String(),
		Qty:       ev.Qty.String(),
		Side:      string(ev.Side),
		MakerId:   ev.MakerID,
		TakerId:   ev.TakerID,
	}
}
