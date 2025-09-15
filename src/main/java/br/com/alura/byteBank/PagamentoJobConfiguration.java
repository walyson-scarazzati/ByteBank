package br.com.alura.byteBank;

import br.com.alura.byteBank.modelo.Pagamento;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.File;

@Configuration
public class PagamentoJobConfiguration {

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Bean
    public Job job(Step importacao, JobRepository jobRepository) {
        return new JobBuilder("importacao", jobRepository)
                .start(importacao)
                .next(moverArquivosStep(jobRepository))
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    public Step importacao(ItemReader<Pagamento> leitor, ItemWriter escrita, JobRepository jobRepository) {
        return new StepBuilder("importacao-pagamento", jobRepository)
                .<Pagamento, Pagamento>chunk(200, transactionManager)
                .reader(leitor)
                .writer(escrita)
                .build();
    }

    @Bean
    public ItemReader<Pagamento> leitor() {
        return new FlatFileItemReaderBuilder<Pagamento>()
                .name("leitor")
                .resource(new FileSystemResource("files/dados_ficticios.csv"))
                .linesToSkip(1)
                .delimited()
                .delimiter("|")
                .names("funcionario", "cpf", "agencia", "conta", "valor", "mesReferencia")
                .targetType(Pagamento.class)
                .build();
    }

    @Bean
    public ItemWriter<Pagamento> escrita(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Pagamento>()
                .dataSource(dataSource)
                .sql(
                        "INSERT INTO pagamento (funcionario, cpf, agencia, conta, valor, mes_referencia)"
                                + " VALUES (:funcionario, :cpf, :agencia, :conta, :valor, :mesReferencia)"
                )
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .build();
    }

    @Bean
    public Tasklet moverArquivosTasklet() {
        return (contribution, chunkContext) -> {
            File pastaOrigem = new File("files");
            File pastaDestino = new File("imported-files");

            if (!pastaDestino.exists()) {
                pastaDestino.mkdirs();
            }

            File[] arquivos = pastaOrigem.listFiles((dir, name) -> name.endsWith(".csv"));

            if (arquivos != null) {
                for (File arquivo : arquivos) {
                    File arquivoDestino = new File(pastaDestino, arquivo.getName());
                    if (arquivo.renameTo(arquivoDestino)) {
                        System.out.println("Arquivo movido: " + arquivo.getName());
                    } else {
                        throw new RuntimeException("Não foi possível mover o arquivo: " + arquivo.getName());
                    }
                }
            }
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Step moverArquivosStep(JobRepository jobRepository) {
        return new StepBuilder("mover-arquivo", jobRepository)
                .tasklet(moverArquivosTasklet(), transactionManager)
                .build();
    }
}
