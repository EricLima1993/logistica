package io.github.ericlima1993.icompras.logistica.subscriber;

import io.github.ericlima1993.icompras.logistica.subscriber.dto.AtualizacaoFaturamentoDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Component
@Slf4j
@RequiredArgsConstructor
public class FaturamentoSubscriber {

    private final ObjectMapper objectMapper;

    @KafkaListener(groupId = "${spring.kafka.consumer.group-id}", topics = "${icompras.config.kafka.topics.pedidos-faturados}")
    public void listen(String json){
        log.info("Recebendo pedido para envio {}", json);

        try {
            var representation = objectMapper.readValue(json, AtualizacaoFaturamentoDTO.class);
        } catch (JacksonException e) {
            log.error("Erro ao ler os dados de pedido.", e);
        }catch (Exception e) {
            log.error("Erro ao preparar pedido para envio", e);
        }
    }
}
