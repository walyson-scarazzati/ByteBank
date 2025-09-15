package br.com.alura.byteBank.modelo;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Entity
@Table
@Getter
@Setter
public class Pagamento {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String funcionario;
    private String cpf;
    private String agencia;
    private String conta;
    private Double valor;
    private String mesReferencia;
}
